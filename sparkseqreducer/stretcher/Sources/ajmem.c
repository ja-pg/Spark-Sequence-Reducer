/* @source ajmem **************************************************************
**
** AJAX memory functions
**
** @author Copyright (C) 1999 Peter Rice
** @version $Revision: 1.35 $
** @modified Peter Rice pmr@ebi.ac.uk
** @modified $Date: 2011/10/23 20:09:49 $ by $Author: mks $
** @modified $Date: 2018/10/6 $ by $Author: ja-pg $
**
** This library is free software; you can redistribute it and/or
** modify it under the terms of the GNU Lesser General Public
** License as published by the Free Software Foundation; either
** version 2.1 of the License, or (at your option) any later version.
**
** This library is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
** Lesser General Public License for more details.
**
** You should have received a copy of the GNU Lesser General Public
** License along with this library; if not, write to the Free Software
** Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
** MA  02110-1301,  USA.
**
******************************************************************************/

/* ========================================================================= */
/* ============================= include files ============================= */
/* ========================================================================= */

#include "ajmem.h"
#include "ajassert.h"
#include "ajexcept.h"
#include "ajmess.h"
#include "ajutil.h"

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef HAVE_MCHECK
#include <mcheck.h>
#endif /* HAVE_MCHECK */




/* ========================================================================= */
/* =============================== constants =============================== */
/* ========================================================================= */




/* ========================================================================= */
/* =========================== global variables ============================ */
/* ========================================================================= */




/* ========================================================================= */
/* ============================= private data ============================== */
/* ========================================================================= */




/* ========================================================================= */
/* =========================== private constants =========================== */
/* ========================================================================= */




/* @conststatic Mem_Failed ****************************************************
**
** Exception for "Allocation failed, insufficient memory available"
**
******************************************************************************/
static const Except_T Mem_Failed =
{
    "Allocation failed, insufficient memory available"
};





/* @conststatic Mem_Badcount **************************************************
**
** Exception for "Allocation bad byte count"
**
******************************************************************************/
static const Except_T Mem_Badcount =
{
    "Allocation bad byte count"
};




/* ========================================================================= */
/* =========================== private variables =========================== */
/* ========================================================================= */

#ifdef AJ_SAVESTATS
static ajlong memAlloc       = 0;
static ajlong memFree        = 0;
static ajlong memCount       = 0;
static ajlong memResize      = 0;
static ajlong memResizeOld   = 0;
static ajlong memResizeCount = 0;
static ajlong memTotal       = 0;
static ajlong memZero        = 0;
#endif /* AJ_SAVESTATS */

#ifdef HAVE_MCHECK
static void* probePtr = NULL;
static ajint probeLine = 0;
static const char* probeFile = "";
static AjBool probeTest = AJFALSE;
static ajint probeFail = 0;
static ajint probeMaxFail = 0;
#endif /* HAVE_MCHECK */




/* ========================================================================= */
/* =========================== private functions =========================== */
/* ========================================================================= */




/* ========================================================================= */
/* ======================= All functions by section ======================== */
/* ========================================================================= */




/* @func ajMemAlloc ***********************************************************
**
** Allocates memory using malloc, and fails with an error message if
** unsuccessful.
**
** @param [r] nbytes [size_t] Number of bytes required
** @param [r] file [const char*] Source file name, generated by a macro.
** @param [r] line [ajint] Source line number, generated by a macro.
** @param [r] nofail [AjBool] If true, return with a NULL pointer when
**                            unable to allocate.
** @return [void*] Successfully allocated memory, or NULL on failure.
**                            Normal behaviour is to
**                            raise an exception and fail, or if running
**                            with Java, to print to standard error
**                            and exit.
**
** @release 1.0.0
** @@
******************************************************************************/

void* ajMemAlloc(size_t nbytes, const char* file, ajint line, AjBool nofail)
{
    void *ptr;

#ifdef HAVE_JAVA
    (void) file;                /* make it used */
    (void) line;                /* make it used */
#endif /* HAVE_JAVA */

    if(nbytes <= 0)
    {
#ifdef HAVE_JAVA
	fprintf(stderr,"Attempt to allocate <=0 bytes");
	exit(-1);
#else /* !HAVE_JAVA */
	ajExceptRaise(&Mem_Badcount, file, line);
#endif /* !HAVE_JAVA */
    }

    ptr = malloc(nbytes);

    if(ptr == NULL)
    {
	if (nofail)
	    return NULL;
#ifdef HAVE_JAVA
	fprintf(stderr,"Memory allocation failed in ajMemAlloc");
	exit(-1);
#else /* !HAVE_JAVA */
	if(file == NULL)
	    AJRAISE(Mem_Failed);
	else
	    ajExceptRaise(&Mem_Failed, file, line);
#endif /* !HAVE_JAVA */
    }

#ifdef AJ_SAVESTATS
    memAlloc += nbytes;
    memCount++;
    memTotal++;
#endif /* AJ_SAVESTATS */

    return ptr;
}




/* @func ajMemCalloc **********************************************************
**
** Allocates memory using calloc for an array of elements,
** and fails with an error message if unsuccessful.
**
** @param [r] count [size_t] Number of elements required
** @param [r] nbytes [size_t] Number of bytes required per element
** @param [r] file [const char*] Source file name, generated by a macro.
** @param [r] line [ajint] Source line number, generated by a macro.
** @param [r] nofail [AjBool] If true, return with a NULL pointer when
**                            unable to allocate.
** @return [void*] Successfully allocated memory, or NULL on failure.
**                            Normal behaviour is to
**                            raise an exception and fail, or if running
**                            with Java, to print to standard error
**                            and exit.
**
** @release 1.0.0
** @@
******************************************************************************/

void* ajMemCalloc(size_t count, size_t nbytes,
		  const char* file, ajint line, AjBool nofail)
{
    void *ptr;
    size_t ibytes = nbytes;
    size_t icount = count;

#ifdef HAVE_JAVA
    (void) file;                /* make it used */
    (void) line;                /* make it used */
#endif /* HAVE_JAVA */

    if(count <= 0)
        ajUtilCatch();

    if(!count)
       icount = 1;
    if(!nbytes)
        ibytes = 1;

    ptr = calloc(icount, ibytes);

    if(ptr == NULL)
    {
	if (nofail)
	    return NULL;
#ifdef HAVE_JAVA
	fprintf(stderr,"Memory allocation failed in ajMemCalloc");
	exit(-1);
#else /* !HAVE_JAVA */
	if(file == NULL)
	    AJRAISE(Mem_Failed);
	else
	    ajExceptRaise(&Mem_Failed, file, line);
#endif /* !HAVE_JAVA */
    }

#ifdef AJ_SAVESTATS
    memAlloc += (icount*ibytes);
    memCount++;
    memTotal++;
#endif /* AJ_SAVESTATS */

    return ptr;
}




/* @func ajMemCallocZero ******************************************************
**
** Allocates memory using calloc for an array of elements,
** and fails with an error message if unsuccessful.
**
** The memory is initialised to zero. This should be done by the standard
** calloc function. It is explicitly done here to make sure.
**
** @param [r] count [size_t] Number of elements required
** @param [r] nbytes [size_t] Number of bytes required
** @param [r] file [const char*] Source file name, generated by a macro.
** @param [r] line [ajint] Source line number, generated by a macro.
** @param [r] nofail [AjBool] If true, return with a NULL pointer when
**                            unable to allocate.
** @return [void*] Successfully allocated memory, or NULL on failure.
**                            Normal behaviour is to
**                            raise an exception and fail, or if running
**                            with Java, to print to standard error
**                            and exit.
**
** @release 6.0.0
** @@
******************************************************************************/

void* ajMemCallocZero(size_t count, size_t nbytes,
		      const char* file, ajint line, AjBool nofail)
{
    void *ptr;
    size_t ibytes = nbytes;
    size_t icount = count;

#ifdef HAVE_JAVA
    (void) file;                /* make it used */
    (void) line;                /* make it used */
#endif /* HAVE_JAVA */

    if(count <= 0)
        ajUtilCatch();

    if(!count)
       icount = 1;
    if(!nbytes)
        ibytes = 1;

    ptr = calloc(icount, ibytes);

    if(ptr == NULL)
    {
	if (nofail)
	    return NULL;
#ifdef HAVE_JAVA
	fprintf(stderr,"Memory allocation failed in ajMemCallocZero");
	exit(-1);
#else /* !HAVE_JAVA */
	if(file == NULL)
	    AJRAISE(Mem_Failed);
	else
	    ajExceptRaise(&Mem_Failed, file, line);
#endif /* !HAVE_JAVA */
    }

    memset(ptr, 0, icount*ibytes);

#ifdef AJ_SAVESTATS
    memAlloc += (icount*ibytes);
    memCount++;
    memTotal++;
    memZero += (icount*ibytes);
#endif /* AJ_SAVESTATS */

    return ptr;
}




/* @func ajMemSetZero *********************************************************
**
** Zeroes memory for an array of elements,
**
** @param [u] ptr [void*] Pointer to memory previously allocated with 'malloc'
** @param [r] count [size_t] Number of elements required
** @param [r] nbytes [size_t] Number of bytes required
** @return [void]
**
** @release 6.0.0
** @@
******************************************************************************/

void ajMemSetZero(void* ptr, size_t count, size_t nbytes)
{
    if (ptr == NULL)
	return;

    if(!nbytes)
        return;

    if(!count)
        return;

    memset(ptr, 0, count*nbytes);

#ifdef AJ_SAVESTATS
    memZero += (count*nbytes);
#endif /* AJ_SAVESTATS */

    return;
}




/* @func ajMemFree ************************************************************
**
** Frees memory using 'free' and zeroes the pointer. Ignores NULL
** (uninitialised) pointers.
**
** @param [u] ptr [void**] Pointer to memory previously allocated with 'malloc'
**
** @release 1.0.0
** @@
******************************************************************************/

void ajMemFree(void** ptr)
{
    if(!ptr)
        return;
    if(!*ptr)
        return;
        
    free(*ptr);
    *ptr = NULL;

#ifdef AJ_SAVESTATS
    memCount--;
    memFree++;
#endif /* AJ_SAVESTATS */

    return;
}




/* @func ajMemResize **********************************************************
**
** Resizes previously allocated memory, and ensures data is copied to
** the new location if it is moved.
**
** If the pointer is new then new memory is allocated automatically.
**
** The C run-time library function realloc does preserve existing values
** but does not initialise any memory after the old contents.
**
** @param [u] ptr [void*] Pointer to memory previously allocated with 'malloc'
** @param [r] nbytes [size_t] Number of bytes required
** @param [r] file [const char*] Source file name, generated by a macro.
** @param [r] line [ajint] Source line number, generated by a macro.
** @param [r] nofail [AjBool] If true, return with a NULL pointer when
**                            unable to allocate.
** @return [void*] Successfully reallocated memory, or NULL on failure.
**                            Normal behaviour is to
**                            raise an exception and fail, or if running
**                            with Java, to print to standard error
**                            and exit.
**
** @release 1.0.0
** @@
******************************************************************************/

void* ajMemResize(void* ptr, size_t nbytes,
		  const char* file, ajint line, AjBool nofail)
{
    size_t ibytes = nbytes;

    if(!nbytes)
        ibytes = 1;

    if(ptr == NULL)
    {
	ptr = ajMemCallocZero(ibytes, 1, file, line, nofail);

	return ptr;
    }

    ptr = realloc(ptr, ibytes);

    if(ptr == NULL)
    {
	if (nofail)
	    return NULL;
#ifdef HAVE_JAVA
	fprintf(stderr,"Memory allocation failed in ajMemResize");
	exit(-1);
#else /* !HAVE_JAVA */
	if(file == NULL)
	    AJRAISE(Mem_Failed);
	else
	    ajExceptRaise(&Mem_Failed, file, line);
#endif /* !HAVE_JAVA */
    }
  
#ifdef AJ_SAVESTATS
    memResize += ibytes;
    memResizeCount++;
#endif /* AJ_SAVESTATS */
    
    return ptr;
}




/* @func ajMemResizeZero ******************************************************
**
** Resizes previously allocated memory, and ensures data is copied to
** the new location if it is moved.
**
** If the pointer is new then new memory is allocated automatically.
**
** Memory beyond the previous contents is initialised to zero
**
** The C run-time library function realloc does preserves existing values
** but does not initialise any memory after the old contents. This is why
** this function needs to be told what the old size was.
**
** @param [u] ptr [void*] Pointer to memory previously allocated with 'malloc'
** @param [r] oldbytes [size_t] Number of bytes required
** @param [r] nbytes [size_t] Number of bytes required
** @param [r] file [const char*] Source file name, generated by a macro.
** @param [r] line [ajint] Source line number, generated by a macro.
** @param [r] nofail [AjBool] If true, return with a NULL pointer when
**                            unable to allocate.
** @return [void*] Successfully reallocated memory, or NULL on failure.
**                            Normal behaviour is to
**                            raise an exception and fail, or if running
**                            with Java, to print to standard error
**                            and exit.
**
** @release 6.0.0
** @@
******************************************************************************/

void* ajMemResizeZero(void* ptr, size_t oldbytes, size_t nbytes,
		      const char* file, ajint line, AjBool nofail)
{
    size_t ibytes = nbytes;

    if(!nbytes)
        ibytes = 1;

    if(ptr == NULL)
    {
	ptr = ajMemCallocZero(ibytes, 1, file, line, nofail);

	return ptr;
    }

    ptr = realloc(ptr, ibytes);

    if(ptr == NULL)
    {
	if (nofail)
	    return NULL;
#ifdef HAVE_JAVA
	fprintf(stderr,"Memory allocation failed in ajMemResizeZero");
	exit(-1);
#else /* !HAVE_JAVA */
	if(file == NULL)
	    AJRAISE(Mem_Failed);
	else
	    ajExceptRaise(&Mem_Failed, file, line);
#endif /* !HAVE_JAVA */
    }
  
    if(ibytes > oldbytes)
	memset(((char*)ptr)+oldbytes, 0, (nbytes-oldbytes));

#ifdef AJ_SAVESTATS
    memResizeOld += oldbytes;
    memResize += ibytes;
    memResizeCount++;
#endif /* AJ_SAVESTATS */

    return ptr;
}




/* @func ajMemArrB ************************************************************
**
** Creates an AjBool array.
** Use AJFREE to free the memory when no longer needed.
**
** @param [r] size [size_t] Number of array elements.
** @return [ajint*] Newly allocated array.
**
** @release 1.0.0
** @@
******************************************************************************/

ajint* ajMemArrB(size_t size)
{
    return AJCALLOC(size, sizeof(AjBool));
}




/* @func ajMemArrI ************************************************************
**
** Creates an integer array.
** Use AJFREE to free the memory when no longer needed.
**
** @param [r] size [size_t] Number of array elements.
** @return [ajint*] Newly allocated array.
**
** @release 1.0.0
** @@
******************************************************************************/

ajint* ajMemArrI(size_t size)
{
    return AJCALLOC(size, sizeof(ajint));
}




/* @func ajMemArrF ************************************************************
**
** Creates a float array.
** Use AJFREE to free the memory when no longer needed.
**
** @param [r] size [size_t] Number of array elements.
** @return [float*] Newly allocated array.
**
** @release 1.0.0
** @@
******************************************************************************/

float* ajMemArrF(size_t size)
{
    return AJCALLOC(size, sizeof(float));
}




/* @func ajMemStat ************************************************************
**
** Prints a summary of memory usage with debug calls
**
** @param [r] title [const char*] Title for this summary
** @return [void]
**
** @release 1.0.0
** @@
******************************************************************************/

void ajMemStat(const char* title)
{
#ifdef AJ_SAVESTATS
    static ajlong statAlloc       = 0;
    static ajlong statCount       = 0;
    static ajlong statFree        = 0;
    static ajlong statResize      = 0;
    static ajlong statResizeOld   = 0;
    static ajlong statResizeCount = 0;
    static ajlong statTotal       = 0;
    static ajlong statZero        = 0;

    ajDebug("Memory usage since last call %s:\n", title);
    ajDebug("Memory usage (bytes): %Ld allocated, %Ld reallocated "
	    "%Ld returned %Ld zeroed\n",
	    memAlloc - statAlloc, memResize - statResize,
	    memResizeOld - statResizeOld, memZero - statZero);
    ajDebug("Memory usage (number): %Ld allocates, "
	    "%Ld frees, %Ld resizes, %Ld in use\n",
	    memTotal - statTotal, memFree - statFree,
	    memResizeCount - statResizeCount, memCount - statCount);

    statAlloc  = memAlloc;
    statCount  = memCount;
    statFree   = memFree;
    statResize = memResize;
    statTotal  = memTotal;
    statZero   = memZero;

    statResizeCount = memResizeCount;
#else /* !AJ_SAVESTATS */
    (void) title;               /* make it used */
#endif /* !AJ_SAVESTATS */

    return;
}




/* @func ajMemExit ************************************************************
**
** Prints a summary of memory usage with debug calls
**
** @return [void]
**
** @release 1.0.0
** @@
******************************************************************************/

void ajMemExit(void)
{
#ifdef AJ_SAVESTATS
    ajDebug("Memory usage (bytes): %Ld allocated, %Ld reallocated "
	    "%Ld returned %Ld zeroed\n",
	    memAlloc, memResize,memResizeOld,  memZero);
    ajDebug("Memory usage (number): %Ld allocates, "
	    "%Ld frees, %Ld resizes, %Ld in use\n",
	    memTotal, memFree, memResizeCount, memCount);
#endif /* AJ_SAVESTATS */

    return;
}




/* @func ajMemCheck ***********************************************************
**
** Prints a message appropriate to the memcheck status
**
** @param [r] istat [int] Enumerated value from mprobe
** @return [void]
**
** @release 6.0.0
** @@
******************************************************************************/
void ajMemCheck(int istat)
{
#ifdef HAVE_MCHECK
    if(probeTest)
    {
	if(istat == MCHECK_OK)
	    ajWarn("ajMemProbe called for %x in %s line %d",
		   probePtr, probeFile, probeLine);
	else
	    ajErr("ajMemProbe called for %x in %s line %d",
		  probePtr, probeFile, probeLine);
    }

    if(istat == MCHECK_DISABLED)
	ajUser("mcheck is disabled\n");
    else if(istat == MCHECK_OK)
	ajUser("mcheck OK\n");
    else if(istat == MCHECK_HEAD)
	ajUser("mcheck data before was modified\n");
    else if(istat == MCHECK_TAIL)
	ajUser("mcheck data after was modified\n");
    else if(istat == MCHECK_FREE)
	ajUser("mcheck data already freed\n");
    else
	ajUser("mcheck ... SOMETHING UNEXPECTED\n");


    if(probeMaxFail && (++probeFail >= probeMaxFail))
       ajDie("Maximum ajMemProbe failures %d", probeFail);
#else /* !HAVE_MCHECK */
    (void) istat;
#endif /* !HAVE_MCHECK */

    return;
}




/* @func ajMemCheckSetLimit ***************************************************
**
** Prints a message appropriate to the memcheck status
**
** @param [r] maxfail [ajint] Maximum failures allowed
** @return [void]
**
** @release 6.0.0
** @@
******************************************************************************/
void ajMemCheckSetLimit(ajint maxfail)
{
#ifdef HAVE_MCHECK
    if(probeFail >= maxfail)
      ajDie("Maximum ajMemProbe failures set to %d, already at %d",
	    maxfail, probeFail);

    probeMaxFail = maxfail;
#else /* !HAVE_MCHECK */
    (void) maxfail;
#endif /* !HAVE_MCHECK */

    return;
}




/* @func ajMemProbe ***********************************************************
**
** Probes a memory location for possible errors
**
** @param [u] ptr [void*] Pointer to memory previously allocated with 'malloc'
** @param [r] file [const char*] Source file name, generated by a macro.
** @param [r] line [ajint] Source line number, generated by a macro.
** @return [void]
**
** @release 6.0.0
** @@
******************************************************************************/

void ajMemProbe(void* ptr,
		const char* file, ajint line)
{
#ifdef HAVE_MCHECK
    if(ptr == NULL)
    {
	ajWarn("ajMemProbe address %x NULL in %s at line %d", ptr, file, line);

	return;
    }

    probeLine = line;
    probeFile = file;
    probeTest = ajTrue;
    probePtr = ptr;

    mprobe(ptr);

    probeTest = ajFalse;
#else /* !HAVE_MCHECK */
    (void) ptr;
    (void) file;
    (void) line;
#endif /* !HAVE_MCHECK */

    return;
}




#ifdef AJ_COMPILE_DEPRECATED_BOOK
#endif




#ifdef AJ_COMPILE_DEPRECATED
/* @obsolete ajMemCalloc0
** @rename ajMemCallocZero
*/

__deprecated void* ajMemCalloc0(size_t count, size_t nbytes,
				   const char* file, ajint line, AjBool nofail)
{
  return ajMemCallocZero(count, nbytes, file, line, nofail);
}

#endif
