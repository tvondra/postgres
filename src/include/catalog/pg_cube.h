/*-------------------------------------------------------------------------
 *
 * pg_cube.h
 *	  definition of the system "cube" relation (pg_cube) with info about
 *	  pre-aggregated cubes.
 *
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_cube.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CUBE_H
#define PG_CUBE_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_cube definition.  cpp turns this into
 *		typedef struct FormData_pg_cube.
 * ----------------
 */
#define CubeRelationId  4001

CATALOG(pg_cube,4001) BKI_WITHOUT_OIDS BKI_SCHEMA_MACRO
{
	Oid				cubeid;			/* OID of the cube */
	Oid				cuberelid;		/* OID of the relation the cube is defined on */
	Oid				cubechsetid;	/* OID of the changeset used by the cube */
                
	int16			cubenatts;		/* number of columns in cube */
	
	/* variable-length fields start here, but we allow direct access to them */
	int2vector		cubekey;		/* column numbers of cube dimensions */

#ifdef CATALOG_VARLEN
	oidvector		cubecollation;	/* collation identifiers */
	oidvector		cubeclass;		/* opclass identifiers */
	pg_node_tree	cubeexprs;		/* expression trees for cube attributes that
									 * are not simple column references; one for
									 * each zero entry in cubekey[] */

	/*
	 * XXX might be useful to add 'cubepred' to define filtered cubes (on top
	 * of filtered changesets.
	 */
#endif
} FormData_pg_cube;

/* ----------------
 *		Form_pg_cube corresponds to a pointer to a tuple with
 *		the format of pg_cube relation.
 * ----------------
 */
typedef FormData_pg_cube *Form_pg_cube;

/* ----------------
 *		compiler constants for pg_cube
 * ----------------
 */
#define Natts_pg_cube					8
#define Anum_pg_cube_cubeid				1
#define Anum_pg_cube_cuberelid			2
#define Anum_pg_cube_cubechsetid		3
#define Anum_pg_cube_cubenatts			4
#define Anum_pg_cube_cubekey			5
#define Anum_pg_cube_cubeexprs			6
#define Anum_pg_cube_cubecollation		7
#define Anum_pg_cube_cubeclass			8

#endif   /* PG_CUBE_H */
