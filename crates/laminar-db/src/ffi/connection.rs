//! FFI connection functions.
//!
//! Provides `extern "C"` wrappers for database connection operations.

use std::ffi::{c_char, CStr};
use std::ptr;

use crate::api::Connection;

use super::error::{
    clear_last_error, set_last_error, LAMINAR_ERR_INVALID_UTF8, LAMINAR_ERR_NULL_POINTER,
    LAMINAR_OK,
};
use super::query::{LaminarQueryResult, LaminarQueryStream};

/// Opaque connection handle for FFI.
///
/// This wraps a `Connection` for C callers. Create with `laminar_open()`,
/// free with `laminar_close()`.
#[repr(C)]
pub struct LaminarConnection {
    pub(crate) inner: Connection,
}

/// Open a new database connection.
///
/// # Arguments
///
/// * `out` - Pointer to receive the connection handle
///
/// # Returns
///
/// `LAMINAR_OK` on success, or an error code. On error, call `laminar_last_error()`
/// for details.
///
/// # Safety
///
/// `out` must be a valid pointer to a `*mut LaminarConnection`.
#[no_mangle]
pub unsafe extern "C" fn laminar_open(out: *mut *mut LaminarConnection) -> i32 {
    clear_last_error();

    if out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    match Connection::open() {
        Ok(conn) => {
            let handle = Box::new(LaminarConnection { inner: conn });
            // SAFETY: out is non-null (checked above)
            unsafe { *out = Box::into_raw(handle) };
            LAMINAR_OK
        }
        Err(e) => {
            let code = e.code();
            set_last_error(e);
            code
        }
    }
}

/// Close a database connection.
///
/// This frees the connection and all associated resources. The connection
/// handle becomes invalid after this call.
///
/// # Arguments
///
/// * `conn` - Connection handle to close
///
/// # Returns
///
/// `LAMINAR_OK` on success, or an error code.
///
/// # Safety
///
/// `conn` must be a valid handle from `laminar_open()` that has not been closed.
#[no_mangle]
pub unsafe extern "C" fn laminar_close(conn: *mut LaminarConnection) -> i32 {
    clear_last_error();

    if conn.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    // SAFETY: conn is non-null and was created by laminar_open
    let handle = unsafe { Box::from_raw(conn) };
    match handle.inner.close() {
        Ok(()) => LAMINAR_OK,
        Err(e) => {
            let code = e.code();
            set_last_error(e);
            code
        }
    }
}

/// Execute a SQL statement.
///
/// For DDL statements (CREATE, DROP), `out` may be NULL. For queries,
/// `out` receives a `LaminarQueryResult` handle.
///
/// # Arguments
///
/// * `conn` - Database connection
/// * `sql` - Null-terminated SQL string
/// * `out` - Optional pointer to receive query result (may be NULL for DDL)
///
/// # Returns
///
/// `LAMINAR_OK` on success, or an error code.
///
/// # Safety
///
/// * `conn` must be a valid connection handle
/// * `sql` must be a valid null-terminated UTF-8 string. If not null-terminated,
///   a buffer over-read will occur.
/// * If `out` is non-null, it must be a valid pointer
#[no_mangle]
pub unsafe extern "C" fn laminar_execute(
    conn: *mut LaminarConnection,
    sql: *const c_char,
    out: *mut *mut LaminarQueryResult,
) -> i32 {
    clear_last_error();

    if conn.is_null() || sql.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    // SAFETY: sql is non-null (checked above).
    // Caller must ensure sql is null-terminated.
    let len = unsafe { CStr::from_ptr(sql).to_bytes().len() };
    laminar_execute_len(conn, sql, len, out)
}

/// Execute a SQL statement with explicit length.
///
/// # Arguments
///
/// * `conn` - Database connection
/// * `sql` - SQL string (does not need to be null-terminated)
/// * `len` - Length of the SQL string in bytes
/// * `out` - Optional pointer to receive query result (may be NULL for DDL)
///
/// # Returns
///
/// `LAMINAR_OK` on success, or an error code.
///
/// # Safety
///
/// * `conn` must be a valid connection handle
/// * `sql` must be a valid pointer to at least `len` bytes of UTF-8 encoded text
/// * If `out` is non-null, it must be a valid pointer
#[no_mangle]
pub unsafe extern "C" fn laminar_execute_len(
    conn: *mut LaminarConnection,
    sql: *const c_char,
    len: usize,
    out: *mut *mut LaminarQueryResult,
) -> i32 {
    clear_last_error();

    if conn.is_null() || sql.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    // SAFETY: sql is non-null (checked above) and caller guarantees len bytes
    let sql_slice = unsafe { std::slice::from_raw_parts(sql.cast::<u8>(), len) };
    let Ok(sql_str) = std::str::from_utf8(sql_slice) else {
        return LAMINAR_ERR_INVALID_UTF8;
    };

    // SAFETY: conn is non-null (checked above)
    let conn_ref = unsafe { &(*conn).inner };

    match conn_ref.execute(sql_str) {
        Ok(result) => {
            use crate::api::ExecuteResult;
            match result {
                ExecuteResult::Query(stream) => {
                    // Collect to materialized result
                    match stream.collect() {
                        Ok(query_result) => {
                            if !out.is_null() {
                                let handle = Box::new(LaminarQueryResult::new(query_result));
                                // SAFETY: out is non-null (checked)
                                unsafe { *out = Box::into_raw(handle) };
                            }
                            LAMINAR_OK
                        }
                        Err(e) => {
                            let code = e.code();
                            set_last_error(e);
                            code
                        }
                    }
                }
                ExecuteResult::Metadata(batch) => {
                    if !out.is_null() {
                        let query_result = crate::api::QueryResult::from_batch(batch);
                        let handle = Box::new(LaminarQueryResult::new(query_result));
                        // SAFETY: out is non-null (checked)
                        unsafe { *out = Box::into_raw(handle) };
                    }
                    LAMINAR_OK
                }
                ExecuteResult::Ddl(_) | ExecuteResult::RowsAffected(_) => {
                    // DDL or DML - no result to return
                    if !out.is_null() {
                        // SAFETY: out is non-null
                        unsafe { *out = ptr::null_mut() };
                    }
                    LAMINAR_OK
                }
            }
        }
        Err(e) => {
            let code = e.code();
            set_last_error(e);
            code
        }
    }
}

/// Execute a query and get materialized results.
///
/// This executes the SQL and waits for all results before returning.
///
/// # Arguments
///
/// * `conn` - Database connection
/// * `sql` - Null-terminated SQL string
/// * `out` - Pointer to receive query result
///
/// # Returns
///
/// `LAMINAR_OK` on success, or an error code.
///
/// # Safety
///
/// * `conn` must be a valid connection handle
/// * `sql` must be a valid null-terminated UTF-8 string. If not null-terminated,
///   a buffer over-read will occur.
/// * `out` must be a valid pointer
#[no_mangle]
pub unsafe extern "C" fn laminar_query(
    conn: *mut LaminarConnection,
    sql: *const c_char,
    out: *mut *mut LaminarQueryResult,
) -> i32 {
    clear_last_error();

    if conn.is_null() || sql.is_null() || out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    // SAFETY: sql is non-null (checked above).
    // Caller must ensure sql is null-terminated.
    let len = unsafe { CStr::from_ptr(sql).to_bytes().len() };
    laminar_query_len(conn, sql, len, out)
}

/// Execute a query with materialized results and explicit length.
///
/// # Arguments
///
/// * `conn` - Database connection
/// * `sql` - SQL string (does not need to be null-terminated)
/// * `len` - Length of the SQL string in bytes
/// * `out` - Pointer to receive query result
///
/// # Returns
///
/// `LAMINAR_OK` on success, or an error code.
///
/// # Safety
///
/// * `conn` must be a valid connection handle
/// * `sql` must be a valid pointer to at least `len` bytes of UTF-8 encoded text
/// * `out` must be a valid pointer
#[no_mangle]
pub unsafe extern "C" fn laminar_query_len(
    conn: *mut LaminarConnection,
    sql: *const c_char,
    len: usize,
    out: *mut *mut LaminarQueryResult,
) -> i32 {
    clear_last_error();

    if conn.is_null() || sql.is_null() || out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    // SAFETY: sql is non-null (checked above) and caller guarantees len bytes
    let sql_slice = unsafe { std::slice::from_raw_parts(sql.cast::<u8>(), len) };
    let Ok(sql_str) = std::str::from_utf8(sql_slice) else {
        return LAMINAR_ERR_INVALID_UTF8;
    };

    // SAFETY: conn is non-null (checked above)
    let conn_ref = unsafe { &(*conn).inner };

    match conn_ref.query(sql_str) {
        Ok(result) => {
            let handle = Box::new(LaminarQueryResult::new(result));
            // SAFETY: out is non-null (checked above)
            unsafe { *out = Box::into_raw(handle) };
            LAMINAR_OK
        }
        Err(e) => {
            let code = e.code();
            set_last_error(e);
            code
        }
    }
}

/// Execute a query with streaming results.
///
/// Returns a stream handle for incremental result retrieval.
///
/// # Arguments
///
/// * `conn` - Database connection
/// * `sql` - Null-terminated SQL string
/// * `out` - Pointer to receive query stream
///
/// # Returns
///
/// `LAMINAR_OK` on success, or an error code.
///
/// # Safety
///
/// * `conn` must be a valid connection handle
/// * `sql` must be a valid null-terminated UTF-8 string. If not null-terminated,
///   a buffer over-read will occur.
/// * `out` must be a valid pointer
#[no_mangle]
pub unsafe extern "C" fn laminar_query_stream(
    conn: *mut LaminarConnection,
    sql: *const c_char,
    out: *mut *mut LaminarQueryStream,
) -> i32 {
    clear_last_error();

    if conn.is_null() || sql.is_null() || out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    // SAFETY: sql is non-null (checked above).
    // Caller must ensure sql is null-terminated.
    let len = unsafe { CStr::from_ptr(sql).to_bytes().len() };
    laminar_query_stream_len(conn, sql, len, out)
}

/// Execute a query with streaming results and explicit length.
///
/// # Arguments
///
/// * `conn` - Database connection
/// * `sql` - SQL string (does not need to be null-terminated)
/// * `len` - Length of the SQL string in bytes
/// * `out` - Pointer to receive query stream
///
/// # Returns
///
/// `LAMINAR_OK` on success, or an error code.
///
/// # Safety
///
/// * `conn` must be a valid connection handle
/// * `sql` must be a valid pointer to at least `len` bytes of UTF-8 encoded text
/// * `out` must be a valid pointer
#[no_mangle]
pub unsafe extern "C" fn laminar_query_stream_len(
    conn: *mut LaminarConnection,
    sql: *const c_char,
    len: usize,
    out: *mut *mut LaminarQueryStream,
) -> i32 {
    clear_last_error();

    if conn.is_null() || sql.is_null() || out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    // SAFETY: sql is non-null (checked above) and caller guarantees len bytes
    let sql_slice = unsafe { std::slice::from_raw_parts(sql.cast::<u8>(), len) };
    let Ok(sql_str) = std::str::from_utf8(sql_slice) else {
        return LAMINAR_ERR_INVALID_UTF8;
    };

    // SAFETY: conn is non-null (checked above)
    let conn_ref = unsafe { &(*conn).inner };

    match conn_ref.query_stream(sql_str) {
        Ok(stream) => {
            let handle = Box::new(LaminarQueryStream::new(stream));
            // SAFETY: out is non-null (checked above)
            unsafe { *out = Box::into_raw(handle) };
            LAMINAR_OK
        }
        Err(e) => {
            let code = e.code();
            set_last_error(e);
            code
        }
    }
}

/// Start the streaming pipeline.
///
/// # Arguments
///
/// * `conn` - Database connection
///
/// # Returns
///
/// `LAMINAR_OK` on success, or an error code.
///
/// # Safety
///
/// `conn` must be a valid connection handle.
#[no_mangle]
pub unsafe extern "C" fn laminar_start(conn: *mut LaminarConnection) -> i32 {
    clear_last_error();

    if conn.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    // SAFETY: conn is non-null (checked above)
    let conn_ref = unsafe { &(*conn).inner };

    match conn_ref.start() {
        Ok(()) => LAMINAR_OK,
        Err(e) => {
            let code = e.code();
            set_last_error(e);
            code
        }
    }
}

/// Check if the connection is closed.
///
/// # Arguments
///
/// * `conn` - Database connection
/// * `out` - Pointer to receive result (true if closed)
///
/// # Returns
///
/// `LAMINAR_OK` on success, or an error code.
///
/// # Safety
///
/// * `conn` must be a valid connection handle
/// * `out` must be a valid pointer
#[no_mangle]
pub unsafe extern "C" fn laminar_is_closed(conn: *mut LaminarConnection, out: *mut bool) -> i32 {
    clear_last_error();

    if conn.is_null() || out.is_null() {
        return LAMINAR_ERR_NULL_POINTER;
    }

    // SAFETY: conn and out are non-null (checked above)
    unsafe {
        *out = (*conn).inner.is_closed();
    }
    LAMINAR_OK
}

#[cfg(test)]
#[allow(clippy::borrow_as_ptr)]
mod tests {
    use super::*;

    #[test]
    fn test_open_close() {
        let mut conn: *mut LaminarConnection = ptr::null_mut();
        // SAFETY: Test code with valid pointer
        let rc = unsafe { laminar_open(&mut conn) };
        assert_eq!(rc, LAMINAR_OK);
        assert!(!conn.is_null());

        // SAFETY: conn is valid from laminar_open
        let rc = unsafe { laminar_close(conn) };
        assert_eq!(rc, LAMINAR_OK);
    }

    #[test]
    fn test_open_null_pointer() {
        // SAFETY: Testing null pointer handling
        let rc = unsafe { laminar_open(ptr::null_mut()) };
        assert_eq!(rc, LAMINAR_ERR_NULL_POINTER);
    }

    #[test]
    fn test_close_null_pointer() {
        // SAFETY: Testing null pointer handling
        let rc = unsafe { laminar_close(ptr::null_mut()) };
        assert_eq!(rc, LAMINAR_ERR_NULL_POINTER);
    }

    #[test]
    fn test_execute_create_source() {
        let mut conn: *mut LaminarConnection = ptr::null_mut();
        // SAFETY: Test code with valid pointers
        unsafe {
            let rc = laminar_open(&mut conn);
            assert_eq!(rc, LAMINAR_OK);

            let sql = b"CREATE SOURCE ffi_test (id BIGINT, name VARCHAR)\0";
            let rc = laminar_execute(conn, sql.as_ptr().cast(), ptr::null_mut());
            assert_eq!(rc, LAMINAR_OK);

            laminar_close(conn);
        }
    }

    #[test]
    fn test_execute_len() {
        let mut conn: *mut LaminarConnection = ptr::null_mut();
        // SAFETY: Test code with valid pointers
        unsafe {
            laminar_open(&mut conn);

            // Valid SQL, explicit length, no null terminator needed
            let sql = b"CREATE SOURCE ffi_len_test (id BIGINT)";
            let rc = laminar_execute_len(conn, sql.as_ptr().cast(), sql.len(), ptr::null_mut());
            assert_eq!(rc, LAMINAR_OK);

            // Invalid UTF-8
            let invalid_sql = b"\xFF\xFE\xFD";
            let rc = laminar_execute_len(
                conn,
                invalid_sql.as_ptr().cast(),
                invalid_sql.len(),
                ptr::null_mut(),
            );
            assert_eq!(rc, LAMINAR_ERR_INVALID_UTF8);

            // Partial SQL execution (should fail if SQL is incomplete, but here we just test UTF-8 handling)
            let sql_part = b"CREATE SOURCE ffi_part_test (id BIGINT) extra stuff";
            let len = "CREATE SOURCE ffi_part_test (id BIGINT)".len();
            let rc = laminar_execute_len(conn, sql_part.as_ptr().cast(), len, ptr::null_mut());
            assert_eq!(rc, LAMINAR_OK);

            laminar_close(conn);
        }
    }

    #[test]
    fn test_is_closed() {
        let mut conn: *mut LaminarConnection = ptr::null_mut();
        let mut is_closed = true;

        // SAFETY: Test code with valid pointers
        unsafe {
            laminar_open(&mut conn);
            let rc = laminar_is_closed(conn, &mut is_closed);
            assert_eq!(rc, LAMINAR_OK);
            assert!(!is_closed);

            laminar_close(conn);
        }
    }
}
