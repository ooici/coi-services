/*
 * $Id: precomp.h,v 1.1 2005/06/10 15:48:01 vfrolov Exp $
 *
 * Copyright (c) 2005 Vyacheslav Frolov
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
 * $Log: precomp.h,v $
 * Revision 1.1  2005/06/10 15:48:01  vfrolov
 * Initial revision
 *
 *
 */

#ifndef _PRECOMP_H_
#define _PRECOMP_H_

#include <winsock2.h>
#include <windows.h>

#include <stdio.h>

#pragma warning(disable:4710)
#pragma warning(push, 3)
#include <vector>
#pragma warning(pop)
using namespace std;

#include "utils.h"

#endif /* _PRECOMP_H_ */
