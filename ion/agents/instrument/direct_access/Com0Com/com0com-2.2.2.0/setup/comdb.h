/*
 * $Id: comdb.h,v 1.2 2008/12/25 16:57:33 vfrolov Exp $
 *
 * Copyright (c) 2008 Vyacheslav Frolov
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 *
 * $Log: comdb.h,v $
 * Revision 1.2  2008/12/25 16:57:33  vfrolov
 * Added ComDbQueryNames()
 *
 * Revision 1.1  2008/12/24 15:20:35  vfrolov
 * Initial revision
 *
 */

#ifndef _C0C_COMDB_H_
#define _C0C_COMDB_H_

///////////////////////////////////////////////////////////////
class InfFile;
///////////////////////////////////////////////////////////////
BOOL ComDbGetInUse(const char *pPortName, BOOL &inUse);
void ComDbSync(InfFile &infFile);
DWORD ComDbQueryNames(char *pBuf, DWORD maxChars);
///////////////////////////////////////////////////////////////

#endif /* _C0C_COMDB_H_ */
