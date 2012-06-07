/*
 * $Id: commprop.h,v 1.1 2006/08/23 13:09:15 vfrolov Exp $
 *
 * Copyright (c) 2006 Vyacheslav Frolov
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
 * $Log: commprop.h,v $
 * Revision 1.1  2006/08/23 13:09:15  vfrolov
 * Initial revision
 *
 *
 */

#ifndef _C0C_COMMPROP_H_
#define _C0C_COMMPROP_H_

NTSTATUS GetCommProp(
    PC0C_FDOPORT_EXTENSION pDevExt,
    PVOID pBuf,
    ULONG bufSize,
    PULONG pSize);

#endif /* _C0C_COMMPROP_H_ */
