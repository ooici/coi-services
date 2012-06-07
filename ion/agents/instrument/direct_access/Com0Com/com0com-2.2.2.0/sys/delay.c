/*
 * $Id: delay.c,v 1.11 2008/06/26 13:37:10 vfrolov Exp $
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
 * $Log: delay.c,v $
 * Revision 1.11  2008/06/26 13:37:10  vfrolov
 * Implemented noise emulation
 *
 * Revision 1.10  2008/03/14 15:28:39  vfrolov
 * Implemented ability to get paired port settings with
 * extended IOCTL_SERIAL_LSRMST_INSERT
 *
 * Revision 1.9  2007/07/20 07:59:20  vfrolov
 * Fixed idleCount
 *
 * Revision 1.8  2007/06/09 08:49:47  vfrolov
 * Improved baudrate emulation
 *
 * Revision 1.7  2007/06/01 16:15:02  vfrolov
 * Fixed previous fix
 *
 * Revision 1.6  2007/06/01 08:36:26  vfrolov
 * Changed parameter type for SetWriteDelay()
 *
 * Revision 1.5  2007/01/11 14:50:29  vfrolov
 * Pool functions replaced by
 *   C0C_ALLOCATE_POOL()
 *   C0C_ALLOCATE_POOL_WITH_QUOTA()
 *   C0C_FREE_POOL()
 *
 * Revision 1.4  2006/06/23 11:44:52  vfrolov
 * Mass replacement pDevExt by pIoPort
 *
 * Revision 1.3  2006/06/21 16:23:57  vfrolov
 * Fixed possible BSOD after one port of pair removal
 *
 * Revision 1.2  2006/01/10 10:17:23  vfrolov
 * Implemented flow control and handshaking
 * Implemented IOCTL_SERIAL_SET_XON and IOCTL_SERIAL_SET_XOFF
 * Added setting of HoldReasons, WaitForImmediate and AmountInOutQueue
 *   fields of SERIAL_STATUS for IOCTL_SERIAL_GET_COMMSTATUS
 *
 * Revision 1.1  2005/08/23 15:30:22  vfrolov
 * Initial revision
 *
 */

#include "precomp.h"
#include "delay.h"
#include "noise.h"

/*
 * FILE_ID used by HALT_UNLESS to put it on BSOD
 */
#define FILE_ID 7

VOID WriteDelayRoutine(
    IN PKDPC pDpc,
    IN PVOID deferredContext,
    IN PVOID systemArgument1,
    IN PVOID systemArgument2)
{
  PC0C_IO_PORT pIoPort;
  PC0C_ADAPTIVE_DELAY pWriteDelay;

  UNREFERENCED_PARAMETER(pDpc);
  UNREFERENCED_PARAMETER(systemArgument1);
  UNREFERENCED_PARAMETER(systemArgument2);

  pIoPort = (PC0C_IO_PORT)deferredContext;
  pWriteDelay = pIoPort->pWriteDelay;

  if (pWriteDelay) {
    LIST_ENTRY queueToComplete;
    KIRQL oldIrql;

    InitializeListHead(&queueToComplete);

    KeAcquireSpinLock(pIoPort->pIoLock, &oldIrql);

    if (pWriteDelay->started) {
      pWriteDelay->idleCount++;

      ReadWrite(
          pIoPort->pIoPortRemote, FALSE,
          pIoPort, FALSE,
          &queueToComplete);

      if (pWriteDelay->idleCount > 3) {
        if (pIoPort->brokeCharsProbability > 0 && pIoPort->pIoPortRemote->isOpen) {
          SIZE_T idleChars = GetWriteLimit(pWriteDelay);

          pWriteDelay->idleCount = 0;
          pIoPort->brokeIdleChars += GetBrokenChars(pIoPort->brokeCharsProbability, idleChars);
          pWriteDelay->sentFrames += idleChars;

          if (pIoPort->brokeIdleChars > 0) {
            ReadWrite(
                pIoPort->pIoPortRemote, FALSE,
                pIoPort, FALSE,
                &queueToComplete);
          }
        } else {
          StopWriteDelayTimer(pWriteDelay);
        }
      }
    }

    KeReleaseSpinLock(pIoPort->pIoLock, oldIrql);

    FdoPortCompleteQueue(&queueToComplete);
  }
}

SIZE_T GetWriteLimit(PC0C_ADAPTIVE_DELAY pWriteDelay)
{
  ULONGLONG curTime;
  ULONGLONG maxFrames;

  HALT_UNLESS(pWriteDelay);

  curTime = KeQueryInterruptTime();

  HALT_UNLESS(pWriteDelay->params.decibits_per_frame);

  maxFrames = ((curTime - pWriteDelay->startTime) * pWriteDelay->params.baudRate)/
          (pWriteDelay->params.decibits_per_frame * 1000000L);

  if (maxFrames < pWriteDelay->sentFrames)
    return 0;

  return (SIZE_T)(maxFrames - pWriteDelay->sentFrames);
}

NTSTATUS AllocWriteDelay(PC0C_IO_PORT pIoPort)
{
  PC0C_ADAPTIVE_DELAY pWriteDelay;

  pWriteDelay = (PC0C_ADAPTIVE_DELAY)C0C_ALLOCATE_POOL(NonPagedPool, sizeof(*pWriteDelay));

  if (!pWriteDelay)
    return STATUS_INSUFFICIENT_RESOURCES;

  RtlZeroMemory(pWriteDelay, sizeof(*pWriteDelay));

  KeInitializeTimer(&pWriteDelay->timer);
  KeInitializeDpc(&pWriteDelay->timerDpc, WriteDelayRoutine, pIoPort);

  pIoPort->pWriteDelay = pWriteDelay;

  return STATUS_SUCCESS;
}

VOID FreeWriteDelay(PC0C_IO_PORT pIoPort)
{
  PC0C_ADAPTIVE_DELAY pWriteDelay;

  pWriteDelay = pIoPort->pWriteDelay;

  if (pWriteDelay) {
    pIoPort->pWriteDelay = NULL;
    StopWriteDelayTimer(pWriteDelay);
    C0C_FREE_POOL(pWriteDelay);
  }
}

VOID SetWriteDelay(PC0C_IO_PORT pIoPort)
{
  PC0C_ADAPTIVE_DELAY pWriteDelay;
  C0C_DELAY_PARAMS params;

  pWriteDelay = pIoPort->pWriteDelay;

  if (!pWriteDelay)
    return;

  params.baudRate = pIoPort->baudRate.BaudRate;

  /* Startbit + WordLength */
  params.decibits_per_frame = (1 + pIoPort->lineControl.WordLength) * 10;

  switch (pIoPort->lineControl.Parity) {
  case NO_PARITY:
    break;
  default:
  case ODD_PARITY:
  case EVEN_PARITY:
  case MARK_PARITY:
  case SPACE_PARITY:
    params.decibits_per_frame += 10;
    break;
  }

  switch (pIoPort->lineControl.StopBits) {
  default:
  case STOP_BIT_1:
    params.decibits_per_frame += 10;
    break;
  case STOP_BITS_1_5:
    params.decibits_per_frame += 15;
    break;
  case STOP_BITS_2:
    params.decibits_per_frame += 30;
    break;
  }

  if (pWriteDelay->params.baudRate != params.baudRate ||
      pWriteDelay->params.decibits_per_frame != params.decibits_per_frame)
  {
    pWriteDelay->params = params;
    if (pWriteDelay->started) {
      StopWriteDelayTimer(pWriteDelay);
      StartWriteDelayTimer(pWriteDelay);
    }
  }
}

VOID StartWriteDelayTimer(PC0C_ADAPTIVE_DELAY pWriteDelay)
{
  LARGE_INTEGER dueTime;
  ULONG period;
  ULONG intervals_100ns;

  if (pWriteDelay->started)
    return;

  if (!pWriteDelay->params.baudRate)
    return;

  pWriteDelay->startTime = KeQueryInterruptTime();
  pWriteDelay->sentFrames = 0;
  pWriteDelay->idleCount = 0;

  /* 100-nanosecond intervals per frame */
  intervals_100ns = (pWriteDelay->params.decibits_per_frame * 1000000L)/pWriteDelay->params.baudRate;

  if (!intervals_100ns)
    intervals_100ns = 1;

  period = intervals_100ns/10000;  /* 1-millisecond intervals per frame */

  dueTime.QuadPart = -(LONGLONG)intervals_100ns;

  if (!period)
    period = 1;

  KeSetTimerEx(
      &pWriteDelay->timer,
      dueTime, period,
      &pWriteDelay->timerDpc);

  pWriteDelay->started = TRUE;
}

VOID StopWriteDelayTimer(PC0C_ADAPTIVE_DELAY pWriteDelay)
{
  pWriteDelay->started = FALSE;
  KeCancelTimer(&pWriteDelay->timer);
  KeRemoveQueueDpc(&pWriteDelay->timerDpc);
}
