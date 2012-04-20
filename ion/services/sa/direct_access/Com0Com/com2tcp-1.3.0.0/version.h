/*
 * $Id: version.h,v 1.3 2007/05/07 11:16:18 vfrolov Exp $
 *
 * Copyright (c) 2005-2007 Vyacheslav Frolov
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
 */

#ifndef _C2T_VERSION_H_
#define _C2T_VERSION_H_

#define C2T_COPYRIGHT_YEARS "2005-2007"

#define C2T_V1 1
#define C2T_V2 3
#define C2T_V3 0
#define C2T_V4 0

#define MK_VERSION_STR1(V1, V2, V3, V4) #V1 "." #V2 "." #V3 "." #V4
#define MK_VERSION_STR(V1, V2, V3, V4) MK_VERSION_STR1(V1, V2, V3, V4)

#define C2T_VERSION_STR MK_VERSION_STR(C2T_V1, C2T_V2, C2T_V3, C2T_V4)

#endif /* _C2T_VERSION_H_ */
