/*
 * dbfbridge.prg - Harbour DBF/CDX Bridge for Python
 *
 * Provides C-callable functions for reading/writing FoxPro DBF files
 * with full CDX compound index maintenance via Harbour's DBFCDX RDD.
 *
 * Build (produces platform-specific shared library):
 *   hbmk2 -hbdynvm dbfbridge.prg -o libdbfbridge
 *
 * Output:
 *   macOS:   libdbfbridge.dylib
 *   Linux:   libdbfbridge.so
 *   Windows: libdbfbridge.dll
 *
 * Copyright (c) 2026 IntSys UK Ltd. All rights reserved.
 */

REQUEST DBFCDX
REQUEST DBFFPT

#include "dbinfo.ch"

// ============================================================
// Harbour-level functions (called via C bridge below)
// ============================================================

FUNCTION DBF_INIT()
   rddSetDefault( "DBFCDX" )
   SET AUTOPEN ON           // Auto-open structural CDX
   SET DELETED ON           // Respect soft-deleted records
   SET EXCLUSIVE OFF        // Shared access (multi-user)
   SET SOFTSEEK OFF         // Exact seeks only
   SET EPOCH TO 1950        // Date window for 2-digit years
   // Use VFP-compatible locking scheme for Opera 3 compatibility
   rddInfo( RDDI_LOCKSCHEME, DB_DBFLOCK_VFP )
   RETURN 0

// Open a DBF table in shared read/write mode (CDX auto-opens)
FUNCTION DBF_OPEN( cFile, cAlias )
   LOCAL lSuccess

   IF cAlias == NIL .OR. EMPTY( cAlias )
      cAlias := "WORK"
   ENDIF

   // Close if already open with this alias
   IF SELECT( cAlias ) > 0
      ( cAlias )->( dbCloseArea() )
   ENDIF

   BEGIN SEQUENCE
      USE ( cFile ) ALIAS ( cAlias ) VIA "DBFCDX" SHARED NEW
      lSuccess := !NetErr()
   RECOVER
      lSuccess := .F.
   END SEQUENCE

   RETURN IIF( lSuccess, 0, -1 )

// Open a DBF table in exclusive mode (for PACK/ZAP/REINDEX)
FUNCTION DBF_OPEN_EXCLUSIVE( cFile, cAlias )
   LOCAL lSuccess

   IF cAlias == NIL .OR. EMPTY( cAlias )
      cAlias := "WORK"
   ENDIF

   IF SELECT( cAlias ) > 0
      ( cAlias )->( dbCloseArea() )
   ENDIF

   BEGIN SEQUENCE
      USE ( cFile ) ALIAS ( cAlias ) VIA "DBFCDX" EXCLUSIVE NEW
      lSuccess := !NetErr()
   RECOVER
      lSuccess := .F.
   END SEQUENCE

   RETURN IIF( lSuccess, 0, -1 )

// Close a workarea
FUNCTION DBF_CLOSE( cAlias )
   IF cAlias != NIL .AND. !EMPTY( cAlias ) .AND. SELECT( cAlias ) > 0
      ( cAlias )->( dbCloseArea() )
   ELSEIF SELECT() > 0
      dbCloseArea()
   ENDIF
   RETURN 0

// Close all open workareas
FUNCTION DBF_CLOSE_ALL()
   dbCloseAll()
   RETURN 0

// Append a blank record (auto-locks, CDX indexes auto-update)
FUNCTION DBF_APPEND( cAlias )
   LOCAL lOk

   IF cAlias != NIL .AND. !EMPTY( cAlias )
      SELECT ( cAlias )
   ENDIF

   dbAppend()
   lOk := !NetErr()

   RETURN IIF( lOk, 0, -1 )

// Replace a character field value
FUNCTION DBF_REPLACE_C( cAlias, cField, cValue )
   LOCAL nPos

   IF cAlias != NIL .AND. !EMPTY( cAlias )
      SELECT ( cAlias )
   ENDIF

   nPos := FIELDPOS( cField )
   IF nPos > 0
      FIELDPUT( nPos, cValue )
      RETURN 0
   ENDIF

   RETURN -1

// Replace a numeric field value
FUNCTION DBF_REPLACE_N( cAlias, cField, nValue )
   LOCAL nPos

   IF cAlias != NIL .AND. !EMPTY( cAlias )
      SELECT ( cAlias )
   ENDIF

   nPos := FIELDPOS( cField )
   IF nPos > 0
      FIELDPUT( nPos, nValue )
      RETURN 0
   ENDIF

   RETURN -1

// Replace a date field (expects "YYYYMMDD" string)
FUNCTION DBF_REPLACE_D( cAlias, cField, cDateStr )
   LOCAL nPos

   IF cAlias != NIL .AND. !EMPTY( cAlias )
      SELECT ( cAlias )
   ENDIF

   nPos := FIELDPOS( cField )
   IF nPos > 0
      FIELDPUT( nPos, STOD( cDateStr ) )
      RETURN 0
   ENDIF

   RETURN -1

// Replace a logical field
FUNCTION DBF_REPLACE_L( cAlias, cField, lValue )
   LOCAL nPos

   IF cAlias != NIL .AND. !EMPTY( cAlias )
      SELECT ( cAlias )
   ENDIF

   nPos := FIELDPOS( cField )
   IF nPos > 0
      FIELDPUT( nPos, lValue )
      RETURN 0
   ENDIF

   RETURN -1

// Replace a memo field
FUNCTION DBF_REPLACE_M( cAlias, cField, cValue )
   // Memo fields use the same FIELDPUT as character fields
   RETURN DBF_REPLACE_C( cAlias, cField, cValue )

// Commit changes and unlock current record
FUNCTION DBF_UNLOCK( cAlias )
   IF cAlias != NIL .AND. !EMPTY( cAlias )
      SELECT ( cAlias )
   ENDIF

   dbCommit()
   dbUnlock()
   RETURN 0

// Commit and unlock all workareas
FUNCTION DBF_UNLOCK_ALL()
   dbCommitAll()
   dbUnlockAll()
   RETURN 0

// Lock current record for update
FUNCTION DBF_RLOCK( cAlias )
   IF cAlias != NIL .AND. !EMPTY( cAlias )
      SELECT ( cAlias )
   ENDIF
   RETURN IIF( RLOCK(), 0, -1 )

// Lock entire file
FUNCTION DBF_FLOCK( cAlias )
   IF cAlias != NIL .AND. !EMPTY( cAlias )
      SELECT ( cAlias )
   ENDIF
   RETURN IIF( FLOCK(), 0, -1 )

// ============================================================
// Navigation
// ============================================================

FUNCTION DBF_GOTO_TOP( cAlias )
   IF cAlias != NIL .AND. !EMPTY( cAlias )
      SELECT ( cAlias )
   ENDIF
   dbGoTop()
   RETURN 0

FUNCTION DBF_GOTO_BOTTOM( cAlias )
   IF cAlias != NIL .AND. !EMPTY( cAlias )
      SELECT ( cAlias )
   ENDIF
   dbGoBottom()
   RETURN 0

FUNCTION DBF_GOTO_RECORD( cAlias, nRecNo )
   IF cAlias != NIL .AND. !EMPTY( cAlias )
      SELECT ( cAlias )
   ENDIF
   dbGoto( nRecNo )
   RETURN 0

FUNCTION DBF_SKIP( cAlias, nRecs )
   IF cAlias != NIL .AND. !EMPTY( cAlias )
      SELECT ( cAlias )
   ENDIF
   IF nRecs == NIL
      nRecs := 1
   ENDIF
   dbSkip( nRecs )
   RETURN IIF( EOF(), -1, 0 )

FUNCTION DBF_EOF( cAlias )
   IF cAlias != NIL .AND. !EMPTY( cAlias )
      SELECT ( cAlias )
   ENDIF
   RETURN IIF( EOF(), 1, 0 )

FUNCTION DBF_BOF( cAlias )
   IF cAlias != NIL .AND. !EMPTY( cAlias )
      SELECT ( cAlias )
   ENDIF
   RETURN IIF( BOF(), 1, 0 )

FUNCTION DBF_RECNO( cAlias )
   IF cAlias != NIL .AND. !EMPTY( cAlias )
      SELECT ( cAlias )
   ENDIF
   RETURN RecNo()

FUNCTION DBF_RECCOUNT( cAlias )
   IF cAlias != NIL .AND. !EMPTY( cAlias )
      SELECT ( cAlias )
   ENDIF
   RETURN RecCount()

// ============================================================
// Index Operations
// ============================================================

// Seek on active index tag
FUNCTION DBF_SEEK( cAlias, cKey )
   LOCAL lFound

   IF cAlias != NIL .AND. !EMPTY( cAlias )
      SELECT ( cAlias )
   ENDIF

   lFound := dbSeek( cKey )
   RETURN IIF( lFound, 0, -1 )

// Seek numeric key
FUNCTION DBF_SEEK_N( cAlias, nKey )
   LOCAL lFound

   IF cAlias != NIL .AND. !EMPTY( cAlias )
      SELECT ( cAlias )
   ENDIF

   lFound := dbSeek( nKey )
   RETURN IIF( lFound, 0, -1 )

// Set active index tag
FUNCTION DBF_SET_ORDER( cAlias, cTagName )
   IF cAlias != NIL .AND. !EMPTY( cAlias )
      SELECT ( cAlias )
   ENDIF

   IF EMPTY( cTagName )
      ordSetFocus( 0 )     // Natural record order
   ELSE
      ordSetFocus( cTagName )
   ENDIF
   RETURN 0

// Get current active tag name
FUNCTION DBF_ORDER( cAlias )
   IF cAlias != NIL .AND. !EMPTY( cAlias )
      SELECT ( cAlias )
   ENDIF
   RETURN ordSetFocus()

// Get number of index tags
FUNCTION DBF_TAG_COUNT( cAlias )
   IF cAlias != NIL .AND. !EMPTY( cAlias )
      SELECT ( cAlias )
   ENDIF
   RETURN ordCount()

// Get tag name by ordinal position
FUNCTION DBF_TAG_NAME( cAlias, nPos )
   IF cAlias != NIL .AND. !EMPTY( cAlias )
      SELECT ( cAlias )
   ENDIF
   RETURN ordName( nPos )

// Get tag key expression by ordinal position
FUNCTION DBF_TAG_EXPR( cAlias, nPos )
   IF cAlias != NIL .AND. !EMPTY( cAlias )
      SELECT ( cAlias )
   ENDIF
   RETURN ordKey( nPos )

// Rebuild all indexes
FUNCTION DBF_REINDEX( cAlias )
   IF cAlias != NIL .AND. !EMPTY( cAlias )
      SELECT ( cAlias )
   ENDIF
   ordListRebuild()
   RETURN 0

// ============================================================
// Field Access
// ============================================================

// Get field value as string (universal)
FUNCTION DBF_GET_FIELD( cAlias, cField )
   LOCAL xVal, nPos

   IF cAlias != NIL .AND. !EMPTY( cAlias )
      SELECT ( cAlias )
   ENDIF

   nPos := FIELDPOS( cField )
   IF nPos == 0
      RETURN ""
   ENDIF

   xVal := FIELDGET( nPos )

   SWITCH ValType( xVal )
   CASE "C"
   CASE "M"
      RETURN xVal
   CASE "N"
      RETURN LTRIM( STR( xVal, 20, 6 ) )
   CASE "D"
      RETURN DTOS( xVal )
   CASE "L"
      RETURN IIF( xVal, "T", "F" )
   CASE "U"
      RETURN ""
   END SWITCH

   RETURN ""

// Get numeric field value
FUNCTION DBF_GET_FIELD_N( cAlias, cField )
   LOCAL nPos

   IF cAlias != NIL .AND. !EMPTY( cAlias )
      SELECT ( cAlias )
   ENDIF

   nPos := FIELDPOS( cField )
   IF nPos == 0
      RETURN 0
   ENDIF

   RETURN FIELDGET( nPos )

// Get field count
FUNCTION DBF_FCOUNT( cAlias )
   IF cAlias != NIL .AND. !EMPTY( cAlias )
      SELECT ( cAlias )
   ENDIF
   RETURN FCount()

// Get field name by position
FUNCTION DBF_FNAME( cAlias, nPos )
   IF cAlias != NIL .AND. !EMPTY( cAlias )
      SELECT ( cAlias )
   ENDIF
   RETURN FieldName( nPos )

// Get field type by name
FUNCTION DBF_FTYPE( cAlias, cField )
   LOCAL nPos

   IF cAlias != NIL .AND. !EMPTY( cAlias )
      SELECT ( cAlias )
   ENDIF

   nPos := FIELDPOS( cField )
   IF nPos == 0
      RETURN ""
   ENDIF

   RETURN ValType( FIELDGET( nPos ) )

// ============================================================
// Utility
// ============================================================

// Flush current workarea to disk
FUNCTION DBF_FLUSH( cAlias )
   IF cAlias != NIL .AND. !EMPTY( cAlias )
      SELECT ( cAlias )
   ENDIF
   dbCommit()
   RETURN 0

// Flush all workareas
FUNCTION DBF_FLUSH_ALL()
   dbCommitAll()
   RETURN 0

// Check if a workarea alias is in use
FUNCTION DBF_IS_OPEN( cAlias )
   RETURN IIF( SELECT( cAlias ) > 0, 1, 0 )

// Get last error info (Harbour runtime)
FUNCTION DBF_LAST_ERROR()
   RETURN ""  // Placeholder -- errors returned via return codes

// ============================================================
// C-Level Exports (called from Python via ctypes)
// ============================================================

#pragma BEGINDUMP

#include "hbvm.h"
#include "hbapi.h"
#include "hbapiitm.h"
#include "hbapierr.h"
#include "hbstack.h"

#include <string.h>
#include <stdlib.h>

/* Thread-safety: Harbour VM is single-threaded per process.
 * The Python wrapper must serialise all calls with a mutex. */

static int s_initialized = 0;

/* Helper: call a Harbour function with 0-2 string args, return int */
static int hb_call_si( const char * szFunc, const char * szArg1, const char * szArg2 )
{
   PHB_DYNS pDynSym = hb_dynsymFindName( szFunc );
   if( !pDynSym )
      return -99;

   int nArgs = 0;

   hb_vmPushDynSym( pDynSym );
   hb_vmPushNil();

   if( szArg1 )
   {
      hb_vmPushString( szArg1, strlen( szArg1 ) );
      nArgs++;
   }
   if( szArg2 )
   {
      hb_vmPushString( szArg2, strlen( szArg2 ) );
      nArgs++;
   }

   hb_vmDo( nArgs );
   return hb_parni( -1 );
}

/* Helper: call with string + int args, return int */
static int hb_call_sni( const char * szFunc, const char * szArg1, int nArg2 )
{
   PHB_DYNS pDynSym = hb_dynsymFindName( szFunc );
   if( !pDynSym )
      return -99;

   hb_vmPushDynSym( pDynSym );
   hb_vmPushNil();
   if( szArg1 )
      hb_vmPushString( szArg1, strlen( szArg1 ) );
   else
      hb_vmPushNil();
   hb_vmPushNumInt( ( HB_MAXINT ) nArg2 );
   hb_vmDo( 2 );
   return hb_parni( -1 );
}

/* Helper: call with string arg, return string (pointer valid until next call) */
static const char * hb_call_ss( const char * szFunc, const char * szArg1, const char * szArg2 )
{
   PHB_DYNS pDynSym = hb_dynsymFindName( szFunc );
   if( !pDynSym )
      return "";

   int nArgs = 0;
   hb_vmPushDynSym( pDynSym );
   hb_vmPushNil();

   if( szArg1 )
   {
      hb_vmPushString( szArg1, strlen( szArg1 ) );
      nArgs++;
   }
   if( szArg2 )
   {
      hb_vmPushString( szArg2, strlen( szArg2 ) );
      nArgs++;
   }

   hb_vmDo( nArgs );

   const char * result = hb_parc( -1 );
   return result ? result : "";
}

/* ============================================================
 * Exported C functions (HB_EXPORT makes them visible in the .so/.dll)
 * ============================================================ */

HB_EXPORT int hb_dbf_init( void )
{
   if( !s_initialized )
   {
      hb_vmInit( HB_FALSE );
      s_initialized = 1;
   }
   return hb_call_si( "DBF_INIT", NULL, NULL );
}

HB_EXPORT void hb_dbf_quit( void )
{
   if( s_initialized )
   {
      hb_vmQuit();
      s_initialized = 0;
   }
}

HB_EXPORT int hb_dbf_open( const char * szFile, const char * szAlias )
{
   return hb_call_si( "DBF_OPEN", szFile, szAlias );
}

HB_EXPORT int hb_dbf_open_exclusive( const char * szFile, const char * szAlias )
{
   return hb_call_si( "DBF_OPEN_EXCLUSIVE", szFile, szAlias );
}

HB_EXPORT int hb_dbf_close( const char * szAlias )
{
   return hb_call_si( "DBF_CLOSE", szAlias, NULL );
}

HB_EXPORT int hb_dbf_close_all( void )
{
   return hb_call_si( "DBF_CLOSE_ALL", NULL, NULL );
}

HB_EXPORT int hb_dbf_append( const char * szAlias )
{
   return hb_call_si( "DBF_APPEND", szAlias, NULL );
}

HB_EXPORT int hb_dbf_replace_c( const char * szAlias, const char * szField, const char * szValue )
{
   PHB_DYNS pDynSym = hb_dynsymFindName( "DBF_REPLACE_C" );
   if( !pDynSym )
      return -99;

   hb_vmPushDynSym( pDynSym );
   hb_vmPushNil();
   if( szAlias )
      hb_vmPushString( szAlias, strlen( szAlias ) );
   else
      hb_vmPushNil();
   hb_vmPushString( szField, strlen( szField ) );
   hb_vmPushString( szValue, strlen( szValue ) );
   hb_vmDo( 3 );
   return hb_parni( -1 );
}

HB_EXPORT int hb_dbf_replace_n( const char * szAlias, const char * szField, double dValue )
{
   PHB_DYNS pDynSym = hb_dynsymFindName( "DBF_REPLACE_N" );
   if( !pDynSym )
      return -99;

   hb_vmPushDynSym( pDynSym );
   hb_vmPushNil();
   if( szAlias )
      hb_vmPushString( szAlias, strlen( szAlias ) );
   else
      hb_vmPushNil();
   hb_vmPushString( szField, strlen( szField ) );
   hb_vmPushDouble( dValue, HB_DEFAULT_DECIMALS );
   hb_vmDo( 3 );
   return hb_parni( -1 );
}

HB_EXPORT int hb_dbf_replace_d( const char * szAlias, const char * szField, const char * szDateStr )
{
   PHB_DYNS pDynSym = hb_dynsymFindName( "DBF_REPLACE_D" );
   if( !pDynSym )
      return -99;

   hb_vmPushDynSym( pDynSym );
   hb_vmPushNil();
   if( szAlias )
      hb_vmPushString( szAlias, strlen( szAlias ) );
   else
      hb_vmPushNil();
   hb_vmPushString( szField, strlen( szField ) );
   hb_vmPushString( szDateStr, strlen( szDateStr ) );
   hb_vmDo( 3 );
   return hb_parni( -1 );
}

HB_EXPORT int hb_dbf_replace_l( const char * szAlias, const char * szField, int lValue )
{
   PHB_DYNS pDynSym = hb_dynsymFindName( "DBF_REPLACE_L" );
   if( !pDynSym )
      return -99;

   hb_vmPushDynSym( pDynSym );
   hb_vmPushNil();
   if( szAlias )
      hb_vmPushString( szAlias, strlen( szAlias ) );
   else
      hb_vmPushNil();
   hb_vmPushString( szField, strlen( szField ) );
   hb_vmPushLogical( lValue ? HB_TRUE : HB_FALSE );
   hb_vmDo( 3 );
   return hb_parni( -1 );
}

HB_EXPORT int hb_dbf_replace_m( const char * szAlias, const char * szField, const char * szValue )
{
   return hb_dbf_replace_c( szAlias, szField, szValue );
}

HB_EXPORT int hb_dbf_unlock( const char * szAlias )
{
   return hb_call_si( "DBF_UNLOCK", szAlias, NULL );
}

HB_EXPORT int hb_dbf_unlock_all( void )
{
   return hb_call_si( "DBF_UNLOCK_ALL", NULL, NULL );
}

HB_EXPORT int hb_dbf_rlock( const char * szAlias )
{
   return hb_call_si( "DBF_RLOCK", szAlias, NULL );
}

HB_EXPORT int hb_dbf_flock( const char * szAlias )
{
   return hb_call_si( "DBF_FLOCK", szAlias, NULL );
}

/* Navigation */

HB_EXPORT int hb_dbf_goto_top( const char * szAlias )
{
   return hb_call_si( "DBF_GOTO_TOP", szAlias, NULL );
}

HB_EXPORT int hb_dbf_goto_bottom( const char * szAlias )
{
   return hb_call_si( "DBF_GOTO_BOTTOM", szAlias, NULL );
}

HB_EXPORT int hb_dbf_goto_record( const char * szAlias, int nRecNo )
{
   return hb_call_sni( "DBF_GOTO_RECORD", szAlias, nRecNo );
}

HB_EXPORT int hb_dbf_skip( const char * szAlias, int nRecs )
{
   return hb_call_sni( "DBF_SKIP", szAlias, nRecs );
}

HB_EXPORT int hb_dbf_eof( const char * szAlias )
{
   return hb_call_si( "DBF_EOF", szAlias, NULL );
}

HB_EXPORT int hb_dbf_bof( const char * szAlias )
{
   return hb_call_si( "DBF_BOF", szAlias, NULL );
}

HB_EXPORT int hb_dbf_recno( const char * szAlias )
{
   return hb_call_si( "DBF_RECNO", szAlias, NULL );
}

HB_EXPORT int hb_dbf_reccount( const char * szAlias )
{
   return hb_call_si( "DBF_RECCOUNT", szAlias, NULL );
}

/* Index operations */

HB_EXPORT int hb_dbf_seek( const char * szAlias, const char * szKey )
{
   return hb_call_si( "DBF_SEEK", szAlias, szKey );
}

HB_EXPORT int hb_dbf_seek_n( const char * szAlias, int nKey )
{
   return hb_call_sni( "DBF_SEEK_N", szAlias, nKey );
}

HB_EXPORT int hb_dbf_set_order( const char * szAlias, const char * szTag )
{
   return hb_call_si( "DBF_SET_ORDER", szAlias, szTag );
}

HB_EXPORT const char * hb_dbf_order( const char * szAlias )
{
   return hb_call_ss( "DBF_ORDER", szAlias, NULL );
}

HB_EXPORT int hb_dbf_tag_count( const char * szAlias )
{
   return hb_call_si( "DBF_TAG_COUNT", szAlias, NULL );
}

HB_EXPORT const char * hb_dbf_tag_name( const char * szAlias, int nPos )
{
   PHB_DYNS pDynSym = hb_dynsymFindName( "DBF_TAG_NAME" );
   if( !pDynSym )
      return "";

   hb_vmPushDynSym( pDynSym );
   hb_vmPushNil();
   if( szAlias )
      hb_vmPushString( szAlias, strlen( szAlias ) );
   else
      hb_vmPushNil();
   hb_vmPushNumInt( ( HB_MAXINT ) nPos );
   hb_vmDo( 2 );

   const char * result = hb_parc( -1 );
   return result ? result : "";
}

HB_EXPORT int hb_dbf_reindex( const char * szAlias )
{
   return hb_call_si( "DBF_REINDEX", szAlias, NULL );
}

/* Field access */

HB_EXPORT const char * hb_dbf_get_field( const char * szAlias, const char * szField )
{
   return hb_call_ss( "DBF_GET_FIELD", szAlias, szField );
}

HB_EXPORT double hb_dbf_get_field_n( const char * szAlias, const char * szField )
{
   PHB_DYNS pDynSym = hb_dynsymFindName( "DBF_GET_FIELD_N" );
   if( !pDynSym )
      return 0.0;

   hb_vmPushDynSym( pDynSym );
   hb_vmPushNil();
   if( szAlias )
      hb_vmPushString( szAlias, strlen( szAlias ) );
   else
      hb_vmPushNil();
   hb_vmPushString( szField, strlen( szField ) );
   hb_vmDo( 2 );

   return hb_parnd( -1 );
}

HB_EXPORT int hb_dbf_fcount( const char * szAlias )
{
   return hb_call_si( "DBF_FCOUNT", szAlias, NULL );
}

HB_EXPORT const char * hb_dbf_fname( const char * szAlias, int nPos )
{
   PHB_DYNS pDynSym = hb_dynsymFindName( "DBF_FNAME" );
   if( !pDynSym )
      return "";

   hb_vmPushDynSym( pDynSym );
   hb_vmPushNil();
   if( szAlias )
      hb_vmPushString( szAlias, strlen( szAlias ) );
   else
      hb_vmPushNil();
   hb_vmPushNumInt( ( HB_MAXINT ) nPos );
   hb_vmDo( 2 );

   const char * result = hb_parc( -1 );
   return result ? result : "";
}

HB_EXPORT const char * hb_dbf_ftype( const char * szAlias, const char * szField )
{
   return hb_call_ss( "DBF_FTYPE", szAlias, szField );
}

/* Utility */

HB_EXPORT int hb_dbf_flush( const char * szAlias )
{
   return hb_call_si( "DBF_FLUSH", szAlias, NULL );
}

HB_EXPORT int hb_dbf_flush_all( void )
{
   return hb_call_si( "DBF_FLUSH_ALL", NULL, NULL );
}

HB_EXPORT int hb_dbf_is_open( const char * szAlias )
{
   return hb_call_si( "DBF_IS_OPEN", szAlias, NULL );
}

#pragma ENDDUMP
