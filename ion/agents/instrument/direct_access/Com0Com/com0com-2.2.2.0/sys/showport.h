/*
 * $Id: showport.h,v 1.2 2008/05/04 09:51:45 vfrolov Exp $
 *
 * Copyright (c) 2007-2008 Vyacheslav Frolov
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
 * $Log: showport.h,v $
 * Revision 1.2  2008/05/04 09:51:45  vfrolov
 * Implemented HiddenMode option
 *
 * Revision 1.1  2007/06/01 16:22:40  vfrolov
 * Implemented plug-in and exclusive modes
 *
 */

#ifndef _C0C_SHOWPORT_H_
#define _C0C_SHOWPORT_H_

BOOLEAN HidePortName(IN PC0C_FDOPORT_EXTENSION pDevExt);
BOOLEAN HidePort(IN PC0C_FDOPORT_EXTENSION pDevExt);
BOOLEAN ShowPort(IN PC0C_FDOPORT_EXTENSION pDevExt);
VOID SetHiddenMode(IN PC0C_FDOPORT_EXTENSION pDevExt, ULONG hiddenMode);

#endif /* _C0C_SHOWPORT_H_ */
