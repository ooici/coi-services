/*
 * $Id: delay.h,v 1.6 2008/03/14 15:28:39 vfrolov Exp $
 *
 * Copyright (c) 2005-2008 Vyacheslav Frolov
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
 * $Log: delay.h,v $
 * Revision 1.6  2008/03/14 15:28:39  vfrolov
 * Implemented ability to get paired port settings with
 * extended IOCTL_SERIAL_LSRMST_INSERT
 *
 * Revision 1.5  2007/06/09 08:49:47  vfrolov
 * Improved baudrate emulation
 *
 * Revision 1.4  2007/06/01 08:36:26  vfrolov
 * Changed parameter type for SetWriteDelay()
 *
 * Revision 1.3  2006/06/23 11:44:52  vfrolov
 * Mass replacement pDevExt by pIoPort
 *
 * Revision 1.2  2006/06/21 16:23:57  vfrolov
 * Fixed possible BSOD after one port of pair removal
 *
 * Revision 1.1  2005/08/23 15:30:22  vfrolov
 * Initial revision
 *
 *
 */

#ifndef _C0C_DELAY_H_
#define _C0C_DELAY_H_

typedef struct _C0C_DELAY_PARAMS {
  ULONG                   decibits_per_frame;
  ULONG                   baudRate;
} C0C_DELAY_PARAMS, *PC0C_DELAY_PARAMS;

typedef struct _C0C_ADAPTIVE_DELAY {
  KTIMER                  timer;
  KDPC                    timerDpc;

  C0C_DELAY_PARAMS        params;

  ULONGLONG               startTime;
  ULONGLONG               sentFrames;
  short                   idleCount;
  BOOLEAN                 started;
} C0C_ADAPTIVE_DELAY, *PC0C_ADAPTIVE_DELAY;

NTSTATUS AllocWriteDelay(PC0C_IO_PORT pIoPort);
VOID FreeWriteDelay(PC0C_IO_PORT pIoPort);
SIZE_T GetWriteLimit(PC0C_ADAPTIVE_DELAY pWriteDelay);
VOID SetWriteDelay(PC0C_IO_PORT pIoPort);
VOID StartWriteDelayTimer(PC0C_ADAPTIVE_DELAY pWriteDelay);
VOID StopWriteDelayTimer(PC0C_ADAPTIVE_DELAY pWriteDelay);

#endif /* _C0C_DELAY_H_ */
