/*-------------------------------------------------------------------------
 *
 * monetdb_fdw.c
 *                a monetdb foreign-data wrapper
 *
 * Copyright (c) 2010-2013, PostgreSQL Global Development Group
 * Copyright (c) 2013, Satoshi Nagayasu
 *
 * IDENTIFICATION
 *                contrib/monetdb_fdw/monetdb_fdw.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_table.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/builtins.h"
#include "utils/rel.h"

#include <stdio.h>
#include <mapi.h>

PG_MODULE_MAGIC;

//#define _DEBUG 1

typedef struct MonetdbFdwPlanState
{
	char *host;          /* host */
	char *port;          /* port */
	char *user;          /* user name */
	char *passwd;        /* password */
	char *dbname;        /* database name */
	char *table;         /* target table name */
	char *query;         /* pre-defined query */
	char *monetdb_opt6;              /* required option 2 */
	List *options;                    /* other options */
	
	BlockNumber pages;                      /* estimate of file's physical size */
	double          ntuples;                /* estimate of number of rows in file */
} MonetdbFdwPlanState;

typedef struct MonetdbFdwExecutionState
{
	/*
	 * TODO: Add members here to keep status information while a table scan.
	 *
	 * This struct must be allocated and initialized in BeginForeignScan(),
	 * and would be destroyed in EndForeignScan().
	 *
	 * Generally, resource handler to access external resource needs to be
	 * kept here.
	 */
	Mapi dbh;
	MapiHdl hdl;

	Relation rel;
	int linecount;
} MonetdbFdwExecutionState;

struct MonetdbFdwOption
{
  const char *optname;
  Oid                     optcontext;             /* Oid of catalog in which option may appear */
};

extern Datum monetdb_fdw_handler(PG_FUNCTION_ARGS);
extern Datum monetdb_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(monetdb_fdw_handler);
PG_FUNCTION_INFO_V1(monetdb_fdw_validator);

static const struct MonetdbFdwOption valid_options[] = {
  {"host", ForeignTableRelationId},
  {"port", ForeignTableRelationId},
  {"user", ForeignTableRelationId},
  {"passwd", ForeignTableRelationId},
  {"dbname", ForeignTableRelationId},
  {"table", ForeignTableRelationId},
  {"query", ForeignTableRelationId},
  {"monetdb_opt6", ForeignTableRelationId},

  /* Sentinel */
  {NULL, InvalidOid}
};

static void monetdbGetForeignRelSize(PlannerInfo *, RelOptInfo *, Oid);
static void monetdbGetForeignPaths(PlannerInfo *, RelOptInfo *, Oid);
static ForeignScan *monetdbGetForeignPlan(PlannerInfo *, RelOptInfo *, Oid, ForeignPath *, List *, List *
#if PG_VERSION_NUM >= 90500
, Plan *
#endif
);
static void monetdbExplainForeignScan(ForeignScanState *, ExplainState *);
static void monetdbBeginForeignScan(ForeignScanState *, int);
static TupleTableSlot *monetdbIterateForeignScan(ForeignScanState *);
static void monetdbEndForeignScan(ForeignScanState *);
static void monetdbReScanForeignScan(ForeignScanState *);

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
monetdb_fdw_handler(PG_FUNCTION_ARGS)
{
  FdwRoutine *fdwroutine = makeNode(FdwRoutine);

  fdwroutine->GetForeignRelSize  = monetdbGetForeignRelSize;
  fdwroutine->GetForeignPaths    = monetdbGetForeignPaths;
  fdwroutine->GetForeignPlan     = monetdbGetForeignPlan;
  fdwroutine->ExplainForeignScan = monetdbExplainForeignScan;
  fdwroutine->BeginForeignScan   = monetdbBeginForeignScan;
  fdwroutine->IterateForeignScan = monetdbIterateForeignScan;
  fdwroutine->EndForeignScan     = monetdbEndForeignScan;
  fdwroutine->ReScanForeignScan  = monetdbReScanForeignScan;
  //  fdwroutine->AnalyzeForeignTable = fileAnalyzeForeignTable;

  PG_RETURN_POINTER(fdwroutine);
}

static void
monetdbGetOptions(Oid foreigntableid, char **host, char **port,
				  char **user, char **passwd,
				  char **dbname, char **tablename, char **query)
{
	ForeignTable *table;
	ForeignServer *server;
	ForeignDataWrapper *wrapper;

	List	   *options;
	ListCell   *cell, *prev;

	/*
	 * Extract options from FDW objects.  We ignore user mappings because
	 * file_fdw doesn't have any options that can be specified there.
	 *
	 * (XXX Actually, given the current contents of valid_options[], there's
	 * no point in examining anything except the foreign table's own options.
	 * Simplify?)
	 */
	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	wrapper = GetForeignDataWrapper(server->fdwid);

	options = NIL;
	options = list_concat(options, wrapper->options);
	options = list_concat(options, server->options);
	options = list_concat(options, table->options);

	*host = NULL;
	*port = NULL;
	*user = NULL;
	*passwd = NULL;
	*dbname = NULL;
	*tablename = NULL;
	*query = NULL;
#ifdef NOT_USED
	monetdb_opt6 = NULL;
#endif

 retry:
	prev = NULL;
	foreach(cell, options)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if (strcmp(def->defname, "host") == 0)
		{
			*host = defGetString(def);
			options = list_delete_cell(options, cell, prev);
			goto retry;
		}
		else if (strcmp(def->defname, "port") == 0)
		{
			*port = defGetString(def);
			options = list_delete_cell(options, cell, prev);
			goto retry;
		}
		else if (strcmp(def->defname, "user") == 0)
		{
			*user = defGetString(def);
			options = list_delete_cell(options, cell, prev);
			goto retry;
		}
		else if (strcmp(def->defname, "passwd") == 0)
		{
			*passwd = defGetString(def);
			options = list_delete_cell(options, cell, prev);
			goto retry;
		}
		else if (strcmp(def->defname, "dbname") == 0)
		{
			*dbname = defGetString(def);
			options = list_delete_cell(options, cell, prev);
			goto retry;
		}
		else if (strcmp(def->defname, "table") == 0)
		{
			*tablename = defGetString(def);
			options = list_delete_cell(options, cell, prev);
			goto retry;
		}
		else if (strcmp(def->defname, "query") == 0)
		{
			*query = defGetString(def);
			options = list_delete_cell(options, cell, prev);
			goto retry;
		}
#ifdef NOT_USED
		else if (strcmp(def->defname, "monetdb_opt6") == 0)
		{
			monetdb_opt6 = defGetString(def);
			options = list_delete_cell(options, cell, prev);
			goto retry;
		}
#endif
		else
		  prev = cell;
	}

	/*
	 * Check required option(s) here.
	 */
	if (host == NULL)
		elog(ERROR, "monetdb_fdw: host is required for monetdb_fdw foreign tables");
	if (port == NULL)
		elog(ERROR, "monetdb_fdw: port is required for monetdb_fdw foreign tables");
	if (user == NULL)
		elog(ERROR, "monetdb_fdw: user is required for monetdb_fdw foreign tables");
	if (passwd == NULL)
		elog(ERROR, "monetdb_fdw: passwd is required for monetdb_fdw foreign tables");
	if (dbname == NULL)
		elog(ERROR, "monetdb_fdw: dbname is required for monetdb_fdw foreign tables");
	if (tablename == NULL && query == NULL)
		elog(ERROR, "monetdb_fdw: table or query is required for monetdb_fdw foreign tables");
#ifdef NOT_USED
	if (monetdb_opt6 == NULL)
		elog(ERROR, "monetdb_fdw: monetdb_opt6 is required for monetdb_fdw foreign tables");
#endif

#ifdef _DEBUG
	elog(NOTICE, "monetdb_fdw: host=%s, port=%d, user=%s, pass=XXX, dbname=%s, table=%s, query=%s",
		 *host,
		 atoi(*port),
		 *user,
		 *dbname,
		 *tablename,
		 *query);
#endif

	/* Other options */
//	options = options;
}

static double
monetdbEstimateRowsImpl(PlannerInfo *root,
			 RelOptInfo *baserel,
			 MonetdbFdwPlanState *fdw_private)
{
  //  estimate_size(root, baserel, fdw_private);

  /*
   * TODO: Implement this function to estimate number of rows.
   */
  return atoi(fdw_private->port);
}

/*
 * monetdbGetForeignRelSize
 *
 * This function is intended for estimating relation size, which means
 * number of rows in the table.
 *
 * To estimate number of rows, this function extract fdw options at first,
 * and then, estimate number of rows with using it.
 */
static void
monetdbGetForeignRelSize(PlannerInfo *root,
			  RelOptInfo *baserel,
			  Oid foreigntableid)
{
  MonetdbFdwPlanState *fdw_private;

  /*
   * Fetch options.  We only need filename at this point, but we might as
   * well get everything and not need to re-fetch it later in planning.
   */
  fdw_private = (MonetdbFdwPlanState *) palloc(sizeof(MonetdbFdwPlanState));
 
  monetdbGetOptions(foreigntableid,
					&fdw_private->host,
					&fdw_private->port,
					&fdw_private->user,
					&fdw_private->passwd,
					&fdw_private->dbname,
					&fdw_private->table,
					&fdw_private->query);

  baserel->fdw_private = (void *) fdw_private;

  /* Estimate relation size */
  baserel->rows = monetdbEstimateRowsImpl(root, baserel, fdw_private);
}

static void
monetdbGetForeignPaths(PlannerInfo *root,
			RelOptInfo *baserel,
			Oid foreigntableid)
{
  MonetdbFdwPlanState *fdw_private = (MonetdbFdwPlanState *) baserel->fdw_private;
  Cost            startup_cost = 0;
  Cost            total_cost = 0;
#ifdef NOT_USED
  List       *columns;
#endif
  List       *coptions = NIL;

#ifdef NOT_USED
  /* Decide whether to selectively perform binary conversion */
  if (check_selective_binary_conversion(baserel,
					foreigntableid,
					&columns))
    coptions = list_make1(makeDefElem("convert_selectively",
				      (Node *) columns));

#endif
  /* Estimate costs */
  //  estimate_costs(root, baserel, fdw_private,
  //		 &startup_cost, &total_cost);
  total_cost = 0;

  /*
   * Create a ForeignPath node and add it as only possible path.  We use the
   * fdw_private list of the path to carry the convert_selectively option;
   * it will be propagated into the fdw_private list of the Plan node.
   */
  add_path(baserel, (Path *)
	   create_foreignscan_path(root, baserel,
				   baserel->rows,
				   startup_cost,
				   total_cost,
				   NIL,           /* no pathkeys */
				   NULL,          /* no outer rel either */
#if PG_VERSION_NUM >= 90500
                                   NULL,
#endif
				   coptions));

  /*
   * If data file was sorted, and we knew it somehow, we could insert
   * appropriate pathkeys into the ForeignPath node to tell the planner
   * that.
   */
}

static ForeignScan *
monetdbGetForeignPlan(PlannerInfo *root,
		       RelOptInfo *baserel,
		       Oid foreigntableid,
		       ForeignPath *best_path,
		       List *tlist,
		       List *scan_clauses
#if PG_VERSION_NUM >= 90500
                       ,
                       Plan *outer_plan
#endif
                       )
{
  Index           scan_relid = baserel->relid;

  /*
   * We have no native ability to evaluate restriction clauses, so we just
   * put all the scan_clauses into the plan node's qual list for the
   * executor to check.  So all we have to do here is strip RestrictInfo
   * nodes from the clauses and ignore pseudoconstants (which will be
   * handled elsewhere).
   */
  scan_clauses = extract_actual_clauses(scan_clauses, false);

  /* Create the ForeignScan node */
  return make_foreignscan(tlist,
			  scan_clauses,
			  scan_relid,
			  NIL,    /* no expressions to evaluate */
			  best_path->fdw_private
#if (PG_VERSION_NUM >= 90500)
                         ,NIL /* no scan_tlist either */
                         ,NIL   /* no remote quals */
                         ,outer_plan
#endif
                         );
}

static void
monetdbExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
  //  char       *filename;
  //  List       *options;

  /* Fetch options --- we only need filename at this point */
  //  fileGetOptions(RelationGetRelid(node->ss.ss_currentRelation),
  //		 &filename, &options);

  ExplainPropertyText("Foreign File", "monetdb", es);

  /* Suppress file size if we're not showing cost details */
  if (es->costs)
    {
	ExplainPropertyLong("Foreign File Size", 8192, es);
    }
}

static void
monetdb_die(Mapi dbh, MapiHdl hdl)
{
	char *err;

	if (mapi_result_error(hdl))
		err = pstrdup(mapi_result_error(hdl));
	else if (mapi_error_str(dbh))
		err = pstrdup(mapi_error_str(dbh));
	else
		err = "unknown error.";

	if (hdl != NULL) {
		mapi_explain_query(hdl, stderr);
		do {
			if (mapi_result_error(hdl) != NULL)
				mapi_explain_result(hdl, stderr);
		} while (mapi_next_result(hdl) == 1);
		mapi_close_handle(hdl);
		mapi_destroy(dbh);
	} else if (dbh != NULL) {
		mapi_explain(dbh, stderr);
		mapi_destroy(dbh);
	}

	elog(ERROR, "monetdb_fdw: %s", err);
}

static void
monetdbBeginForeignScan(ForeignScanState *node, int eflags)
{
  ForeignScan *plan = (ForeignScan *) node->ss.ps.plan;
  MonetdbFdwExecutionState *festate;
  MonetdbFdwPlanState fdw_private;

  	char *host;
	char *port;
	char *user;
	char *passwd;
	char *dbname;
	char *table;
	char *query;
	
	monetdbGetOptions(RelationGetRelid(node->ss.ss_currentRelation),
					  &host, &port, &user, &passwd, &dbname, &table, &query);

  /*
   * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
   */
  if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
    return;

  /*
   * Save state in node->fdw_state.  We must save enough information to call
   * BeginCopyFrom() again.
   */
  festate = (MonetdbFdwExecutionState *) palloc(sizeof(MonetdbFdwExecutionState));

  festate->dbh = NULL;
  festate->hdl = NULL;

  /*
   * TODO: Open an external table (or resource) here.
   */
  festate->dbh = mapi_connect(host, atoi(port), user, passwd, "sql", dbname);
  if (mapi_error(festate->dbh))
	  monetdb_die(festate->dbh, festate->hdl);

  festate->rel       = node->ss.ss_currentRelation;
  festate->linecount = 0;

  node->fdw_state = (void *) festate;
}

/*
 * buildTupleImpl()
 *
 * TODO: Implement this function to build a single tuple.
 *
 * Read data from an external table, and pack them into
 * a tuple (a Datum array) with using XXXGetDatum().
 * See include/postgres.h for more infomation about
 * XXXGetDatum() functions.
 *
 * Also FdwExecutionState needs to be updated here.
 *
 * Return true if a tuple is found (or available),
 * false if not available (or end of the scan).
 */
//static bool
static HeapTuple
buildTupleImpl(MonetdbFdwExecutionState *festate)
{
	int i;
	char **values;
	HeapTuple tuple;
	int num_attrs = RelationGetDescr(festate->rel)->natts;

	values = (char **)palloc( sizeof(char *) * num_attrs );

//	elog(NOTICE, "buildTupleImpl: num_attrs=%d", num_attrs);

	/* end of result set */
	if ( !mapi_fetch_row(festate->hdl) )
		return NULL;

	for (i=0 ; i<num_attrs ; i++)
	{
		values[i] = mapi_fetch_field(festate->hdl, i);

#ifdef _DEBUG
		elog(NOTICE, "buildTupleImpl: mapi_fetch_field -> %s", values[i]);
#endif
	}

	tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(festate->rel->rd_att), values);

    festate->linecount++;

    return tuple;
}

static void
monetdbErrorCallback(void *arg)
{
	MonetdbFdwExecutionState *festate = (MonetdbFdwExecutionState *)arg;

	errcontext("relation %s, line %d",
		   NameStr(festate->rel->rd_rel->relname),
		   festate->linecount);
}

static TupleTableSlot *
monetdbIterateForeignScan(ForeignScanState *node)
{
  MonetdbFdwExecutionState *festate = (MonetdbFdwExecutionState *) node->fdw_state;
  TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
  bool            found;
  ErrorContextCallback errcallback;

  /* Set up callback to identify error line number. */
  errcallback.callback = monetdbErrorCallback;
  errcallback.arg      = (void *)festate;

  errcallback.previous = error_context_stack;
  error_context_stack  = &errcallback;

  if (!festate->hdl)
  {
	  char *host;
	  char *port;
	  char *user;
	  char *passwd;
	  char *dbname;
	  char *table;
	  char *query;

	  char q[1024];
	  
	  monetdbGetOptions(RelationGetRelid(node->ss.ss_currentRelation),
						&host, &port, &user, &passwd, &dbname, &table, &query);
	  
	  if ( !query )
		  snprintf(q, sizeof(q), "SELECT * FROM %s", table);
	  else
		  strncpy(q, query, sizeof(q));

#ifdef _DEBUG
	  elog(NOTICE, "monetdb_fdw: monetdbIterateForeignScan: query=%s", query);
#endif

	  if ((festate->hdl = mapi_query(festate->dbh, q)) == NULL ||
		  mapi_error(festate->dbh) != MOK)
	  {
		  monetdb_die(festate->dbh, festate->hdl);
	  }

#ifdef _DEBUG
	  elog(NOTICE, "monetdb_fdw: monetdbIterateForeignScan: mapi_query done.");
#endif
  }

  /*
   * The protocol for loading a virtual tuple into a slot is first
   * ExecClearTuple, then fill the values/isnull arrays, then
   * ExecStoreVirtualTuple.  If we don't find another row in the file, we
   * just skip the last step, leaving the slot empty as required.
   *
   * We can pass ExprContext = NULL because we read all columns from the
   * file, so no need to evaluate default expressions.
   *
   * We can also pass tupleOid = NULL because we don't allow oids for
   * foreign tables.
   */
  ExecClearTuple(slot);
  //  found = NextCopyFrom(festate->cstate, NULL,
  //		       slot->tts_values, slot->tts_isnull,
  //		       NULL);

  {
    TupleDesc	tupDesc = RelationGetDescr(festate->rel);
    int num_phys_attrs =  tupDesc->natts;
	HeapTuple tup;

    MemSet(slot->tts_values, 0, num_phys_attrs * sizeof(Datum));
    MemSet(slot->tts_isnull, true, num_phys_attrs * sizeof(bool));

    tup = buildTupleImpl(festate);

	if (tup)
		ExecStoreTuple(tup, slot, InvalidBuffer, false);
  }

  /* Remove error callback. */
  error_context_stack = errcallback.previous;

  return slot;
}

static void
monetdbEndForeignScan(ForeignScanState *node)
{
	MonetdbFdwExecutionState *festate = (MonetdbFdwExecutionState *) node->fdw_state;
	
	/* if festate is NULL, we are in EXPLAIN; nothing to do */
	if (festate)
	{
		/*
		 * TODO: Close the external table (resource), and initialize
		 * FdwExecutionState state.
		 */
        if (festate->hdl)
			mapi_close_handle(festate->hdl);

        if (festate->dbh)
			mapi_destroy(festate->dbh);
    }
}

static void
monetdbReScanForeignScan(ForeignScanState *node)
{
  MonetdbFdwExecutionState *festate = (MonetdbFdwExecutionState *) node->fdw_state;

  /*
   * TODO: Close external resource here.
   */

  /*
   * TODO: Re-open the external table (resource) here.
   */
  festate->linecount = 0;
}


/*
 * Check if the option is valid.
 */
static bool
validate_option(DefElem *def, Oid context)
{
  const struct MonetdbFdwOption *opt;
  bool is_valid = false;

  for (opt = valid_options; opt->optname; opt++)
    {
      if (context == opt->optcontext && strcmp(opt->optname, def->defname) == 0)
	is_valid = true;
    }

  if (!is_valid)
    {
      StringInfoData buf;
      
      /*
       * Unknown option specified, complain about it. Provide a hint
       * with list of valid options for the object.
       */
      initStringInfo(&buf);
      for (opt = valid_options; opt->optname; opt++)
	{
	  if (context == opt->optcontext)
	    appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
			     opt->optname);
	}
      
      ereport(ERROR,
	      (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
	       errmsg("invalid option \"%s\"", def->defname),
	       buf.len > 0
	       ? errhint("Valid options in this context are: %s",
			 buf.data)
	       : errhint("There are no valid options in this context.")));
    }
  
  return is_valid;
}


Datum
monetdb_fdw_validator(PG_FUNCTION_ARGS)
{
  List       *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
  Oid        catalog = PG_GETARG_OID(1);
  char       *host = NULL;
  char       *port = NULL;
  char       *user = NULL;
  char       *passwd = NULL;
  char       *dbname = NULL;
  char       *table = NULL;
  char       *query = NULL;
  char       *monetdb_opt6 = NULL;
  ListCell   *cell;

  /*
   * Only superusers are allowed to set options of a file_fdw foreign table.
   * This is because the filename is one of those options, and we don't want
   * non-superusers to be able to determine which file gets read.
   *
   * Putting this sort of permissions check in a validator is a bit of a
   * crock, but there doesn't seem to be any other place that can enforce
   * the check more cleanly.
   *
   * Note that the valid_options[] array disallows setting filename at any
   * options level other than foreign table --- otherwise there'd still be a
   * security hole.
   */
  if (catalog == ForeignTableRelationId && !superuser())
    ereport(ERROR,
	    (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
	     errmsg("only superuser can change options of a monetdb_fdw foreign table")));

  /*
   * Check that only options supported by monetdb_fdw, and allowed for the
   * current object type, are given.
   */
  foreach(cell, options_list)
    {
      DefElem    *def = (DefElem *) lfirst(cell);

      /*
       * Check if the option is valid with looking up MonetdbFdwOption.
       */
      validate_option(def, catalog);

      /*
       * Separate out filename and force_not_null, since ProcessCopyOptions
       * won't accept them.  (force_not_null only comes in a boolean
       * per-column flavor here.)
       */
      /*
       */
      if (strcmp(def->defname, "host") == 0)
	  {
		  if (host)
			  ereport(ERROR,
					  (errcode(ERRCODE_SYNTAX_ERROR),
					   errmsg("conflicting or redundant options")));
		  
		  /*
		   * Option value can be obtained by useing defGetXXX() function:
		   * typically defGetString(), defGetNumeric(), defGetBoolean() or
		   * defGetInt64().
		   *
		   * See commands/defrem.h for more information about defGetXXX()
		   * functions.
		   */
		  host = defGetString(def);
	  }
      else if (strcmp(def->defname, "port") == 0)
	  {
		  if (port)
			  ereport(ERROR,
					  (errcode(ERRCODE_SYNTAX_ERROR),
					   errmsg("conflicting or redundant options")));
		  
		  port = defGetString(def);
	  }
      else if (strcmp(def->defname, "user") == 0)
	  {
		  if (user)
			  ereport(ERROR,
					  (errcode(ERRCODE_SYNTAX_ERROR),
					   errmsg("conflicting or redundant options")));
		  
		  user = defGetString(def);
	  }
      else if (strcmp(def->defname, "passwd") == 0)
	  {
		  if (passwd)
			  ereport(ERROR,
					  (errcode(ERRCODE_SYNTAX_ERROR),
					   errmsg("conflicting or redundant options")));
		  
		  passwd = defGetString(def);
	  }
      else if (strcmp(def->defname, "dbname") == 0)
	  {
		  if (dbname)
			  ereport(ERROR,
					  (errcode(ERRCODE_SYNTAX_ERROR),
					   errmsg("conflicting or redundant options")));
		  
		  dbname = defGetString(def);
	  }
      else if (strcmp(def->defname, "table") == 0)
	  {
		  if (table)
			  ereport(ERROR,
					  (errcode(ERRCODE_SYNTAX_ERROR),
					   errmsg("conflicting or redundant options")));
		  
		  table = defGetString(def);
	  }
      else if (strcmp(def->defname, "query") == 0)
	  {
		  if (query)
			  ereport(ERROR,
					  (errcode(ERRCODE_SYNTAX_ERROR),
					   errmsg("conflicting or redundant options")));
		  
		  query = defGetString(def);
	  }
#ifdef NOT_USED
      else if (strcmp(def->defname, "monetdb_opt6") == 0)
	  {
		  if (monetdb_opt6)
			  ereport(ERROR,
					  (errcode(ERRCODE_SYNTAX_ERROR),
					   errmsg("conflicting or redundant options")));
		  
		  monetdb_opt6 = defGetString(def);
	  }
#endif
    }

  /*
   * TODO: Check required option(s) here.
   */
  if (catalog == ForeignTableRelationId)
  {
	  if (host == NULL)
		  ereport(ERROR,
				  (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				   errmsg("host is required for monetdb_fdw foreign tables")));
	  
	  if (port == NULL)
		  ereport(ERROR,
				  (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				   errmsg("port is required for monetdb_fdw foreign tables")));

	  if (user == NULL)
		  ereport(ERROR,
				  (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				   errmsg("user is required for monetdb_fdw foreign tables")));

	  if (passwd == NULL)
		  ereport(ERROR,
				  (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				   errmsg("passwd is required for monetdb_fdw foreign tables")));

	  if (dbname == NULL)
		  ereport(ERROR,
				  (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				   errmsg("dbname is required for monetdb_fdw foreign tables")));

	  if (table == NULL && query == NULL)
		  ereport(ERROR,
				  (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				   errmsg("table or query is required for monetdb_fdw foreign tables")));

//	  if (monetdb_opt6 == NULL)
//		  ereport(ERROR,
//				  (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
//				   errmsg("monetdb_opt6 is required for monetdb_fdw foreign tables")));
  }
  
  PG_RETURN_VOID();
}
