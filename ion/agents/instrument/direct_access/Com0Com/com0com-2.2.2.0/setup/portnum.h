/*
 * $Id: portnum.h,v 1.1 2006/11/02 16:07:02 vfrolov Exp $
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
 * $Log: portnum.h,v $
 * Revision 1.1  2006/11/02 16:07:02  vfrolov
 * Initial revision
 *
 *
 */

#ifndef _C0C_PORTNUM_H_
#define _C0C_PORTNUM_H_

int GetPortNum(
    HDEVINFO hDevInfo,
    PSP_DEVINFO_DATA pDevInfoData);

LONG SetPortNum(
    HDEVINFO hDevInfo,
    PSP_DEVINFO_DATA pDevInfoData,
    int num);

#endif /* _C0C_PORTNUM_H_ */
