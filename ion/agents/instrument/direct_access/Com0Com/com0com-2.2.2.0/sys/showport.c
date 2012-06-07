/*
 * $Id: showport.c,v 1.3 2008/12/02 16:10:09 vfrolov Exp $
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
 * $Log: showport.c,v $
 * Revision 1.3  2008/12/02 16:10:09  vfrolov
 * Separated tracing and debuging
 *
 * Revision 1.2  2008/05/04 09:51:45  vfrolov
 * Implemented HiddenMode option
 *
 * Revision 1.1  2007/06/01 16:22:40  vfrolov
 * Implemented plug-in and exclusive modes
 *
 */

#include "precomp.h"
#include "showport.h"

/*
 * FILE_ID used by HALT_UNLESS to put it on BSOD
 */
#define FILE_ID 0xD

#ifndef NTDDI_VERSION
/* ZwDeleteValueKey is missing in old DDKs */
NTSYSAPI NTSTATUS NTAPI ZwDeleteValueKey(IN HANDLE KeyHandle, IN PUNICODE_STRING ValueName);
#endif

BOOLEAN HidePortName(IN PC0C_FDOPORT_EXTENSION pDevExt)
{
  BOOLEAN res;
  HANDLE hKey;
  NTSTATUS status;

  res = TRUE;

  status = IoOpenDeviceRegistryKey(pDevExt->pIoPortLocal->pPhDevObj,
                                   PLUGPLAY_REGKEY_DEVICE,
                                   STANDARD_RIGHTS_WRITE,
                                   &hKey);

  if (NT_SUCCESS(status)) {
    UNICODE_STRING keyName;

    RtlInitUnicodeString(&keyName, L"PortName");

    status = ZwDeleteValueKey(hKey, &keyName);

    if (!NT_SUCCESS(status) && (pDevExt->shown & C0C_SHOW_SETNAME) != 0) {
      res = FALSE;
      Trace0((PC0C_COMMON_EXTENSION)pDevExt, L"HidePortName ZwDeleteValueKey(PortName) FAIL");
    }

    ZwClose(hKey);
  }
  else
  if ((pDevExt->shown & C0C_SHOW_SETNAME) != 0) {
    res = FALSE;
    Trace0((PC0C_COMMON_EXTENSION)pDevExt, L"HidePortName IoOpenDeviceRegistryKey(PLUGPLAY_REGKEY_DEVICE) FAIL");
  }

  pDevExt->shown &= ~C0C_SHOW_SETNAME;

  return res;
}

BOOLEAN HidePort(IN PC0C_FDOPORT_EXTENSION pDevExt)
{
  BOOLEAN res;
  NTSTATUS status;

  if (!pDevExt->shown)
    return TRUE;

  res = TRUE;

  if ((pDevExt->shown & C0C_SHOW_WMIREG) != 0) {
    status = IoWMIRegistrationControl(pDevExt->pDevObj, WMIREG_ACTION_DEREGISTER);
    pDevExt->shown &= ~C0C_SHOW_WMIREG;

    if (!NT_SUCCESS(status)) {
      res = FALSE;
      Trace0((PC0C_COMMON_EXTENSION)pDevExt, L"HidePort IoWMIRegistrationControl FAIL");
    }
  }

  if (pDevExt->symbolicLinkName.Buffer && (pDevExt->shown & C0C_SHOW_INTERFACE) != 0) {
    status = IoSetDeviceInterfaceState(&pDevExt->symbolicLinkName, FALSE);
    pDevExt->shown &= ~C0C_SHOW_INTERFACE;

    if (!NT_SUCCESS(status)) {
      res = FALSE;
      Trace0((PC0C_COMMON_EXTENSION)pDevExt, L"HidePort IoSetDeviceInterfaceState FAIL");
    }
  }

  if (pDevExt->ntDeviceName.Buffer && (pDevExt->shown & C0C_SHOW_DEVICEMAP) != 0) {
    status = RtlDeleteRegistryValue(RTL_REGISTRY_DEVICEMAP, C0C_SERIAL_DEVICEMAP,
                                    pDevExt->ntDeviceName.Buffer);
    pDevExt->shown &= ~C0C_SHOW_DEVICEMAP;

    if (!NT_SUCCESS(status)) {
      res = FALSE;
      Trace0((PC0C_COMMON_EXTENSION)pDevExt, L"HidePort RtlDeleteRegistryValue " C0C_SERIAL_DEVICEMAP L" FAIL");
    }
  }

  if (pDevExt->win32DeviceName.Buffer && (pDevExt->shown & C0C_SHOW_SYMLINK) != 0) {
    status = IoDeleteSymbolicLink(&pDevExt->win32DeviceName);
    pDevExt->shown &= ~C0C_SHOW_SYMLINK;

    if (!NT_SUCCESS(status)) {
      res = FALSE;
      Trace0((PC0C_COMMON_EXTENSION)pDevExt, L"HidePort IoDeleteSymbolicLink FAIL");
    }
  }

  if ((pDevExt->shown & C0C_SHOW_SETNAME) != 0)
    res = (HidePortName(pDevExt) && res);

  pDevExt->shown &= ~C0C_SHOW_SHOWN;

  Trace00((PC0C_COMMON_EXTENSION)pDevExt, L"HidePort - ", res ? L"OK" : L"FAIL");

  return res;
}

BOOLEAN ShowPort(IN PC0C_FDOPORT_EXTENSION pDevExt)
{
  BOOLEAN res;
  NTSTATUS status;

  if ((pDevExt->shown & C0C_SHOW_SHOWN) != 0)
    return TRUE;

  res = TRUE;

  if ((pDevExt->shown & C0C_SHOW_SETNAME) == 0 && (pDevExt->hide & C0C_SHOW_SETNAME) == 0) {
    HANDLE hKey;

    status = IoOpenDeviceRegistryKey(pDevExt->pIoPortLocal->pPhDevObj,
                                     PLUGPLAY_REGKEY_DEVICE,
                                     STANDARD_RIGHTS_WRITE,
                                     &hKey);

    if (status == STATUS_SUCCESS) {
      UNICODE_STRING keyName;

      RtlInitUnicodeString(&keyName, L"PortName");

      status = ZwSetValueKey(hKey,
                             &keyName,
                             0,
                             REG_SZ,
                             pDevExt->portName,
                             (ULONG)((wcslen(pDevExt->portName) + 1) * sizeof(WCHAR)));

      if (NT_SUCCESS(status)) {
        pDevExt->shown |= C0C_SHOW_SETNAME;
      } else {
        res = FALSE;
        Trace0((PC0C_COMMON_EXTENSION)pDevExt, L"ShowPort ZwSetValueKey(PortName) FAIL");
      }

      ZwClose(hKey);
    } else {
      res = FALSE;
      Trace0((PC0C_COMMON_EXTENSION)pDevExt, L"ShowPort IoOpenDeviceRegistryKey(PLUGPLAY_REGKEY_DEVICE) FAIL");
    }
  }

  if (pDevExt->ntDeviceName.Buffer) {
    if (pDevExt->win32DeviceName.Buffer &&
        (pDevExt->shown & C0C_SHOW_SYMLINK) == 0 &&
        (pDevExt->hide & C0C_SHOW_SYMLINK) == 0)
    {
      status = IoCreateSymbolicLink(&pDevExt->win32DeviceName, &pDevExt->ntDeviceName);

      if (NT_SUCCESS(status)) {
        pDevExt->shown |= C0C_SHOW_SYMLINK;
      } else {
        res = FALSE;
        Trace0((PC0C_COMMON_EXTENSION)pDevExt, L"ShowPort IoCreateSymbolicLink FAIL");
      }
    }

    if ((pDevExt->shown & C0C_SHOW_SYMLINK) != 0 &&
        (pDevExt->shown & C0C_SHOW_DEVICEMAP) == 0 &&
        (pDevExt->hide & C0C_SHOW_DEVICEMAP) == 0)
    {
      status = RtlWriteRegistryValue(RTL_REGISTRY_DEVICEMAP, C0C_SERIAL_DEVICEMAP,
                                     pDevExt->ntDeviceName.Buffer, REG_SZ,
                                     pDevExt->portName,
                                     (ULONG)((wcslen(pDevExt->portName) + 1) * sizeof(WCHAR)));

      if (NT_SUCCESS(status)) {
        pDevExt->shown |= C0C_SHOW_DEVICEMAP;
      } else {
        res = FALSE;
        Trace0((PC0C_COMMON_EXTENSION)pDevExt, L"ShowPort RtlWriteRegistryValue " C0C_SERIAL_DEVICEMAP L" FAIL");
      }
    }
  }

  if (pDevExt->symbolicLinkName.Buffer &&
      (pDevExt->shown & C0C_SHOW_INTERFACE) == 0 &&
      (pDevExt->hide & C0C_SHOW_INTERFACE) == 0)
  {
    status = IoSetDeviceInterfaceState(&pDevExt->symbolicLinkName, TRUE);

    if (NT_SUCCESS(status)) {
      pDevExt->shown |= C0C_SHOW_INTERFACE;
    } else {
      res = FALSE;
      Trace0((PC0C_COMMON_EXTENSION)pDevExt, L"ShowPort IoSetDeviceInterfaceState FAIL");
    }
  }

  if ((pDevExt->shown & C0C_SHOW_WMIREG) == 0 && (pDevExt->hide & C0C_SHOW_WMIREG) == 0) {
    status = IoWMIRegistrationControl(pDevExt->pDevObj, WMIREG_ACTION_REGISTER);

    if (NT_SUCCESS(status)) {
      pDevExt->shown |= C0C_SHOW_WMIREG;
    } else {
      res = FALSE;
      Trace0((PC0C_COMMON_EXTENSION)pDevExt, L"ShowPort IoWMIRegistrationControl FAIL");
    }
  }

  pDevExt->shown |= C0C_SHOW_SHOWN;

  Trace00((PC0C_COMMON_EXTENSION)pDevExt, L"ShowPort - ", res ? L"OK" : L"FAIL");

  return res;
}

VOID SetHiddenMode(IN PC0C_FDOPORT_EXTENSION pDevExt, ULONG hiddenMode)
{
  if (hiddenMode == 0xFFFFFFFF)
    pDevExt->hide = (C0C_SHOW_SETNAME|C0C_SHOW_DEVICEMAP|C0C_SHOW_WMIREG);
  else
    pDevExt->hide = (UCHAR)hiddenMode;

#if ENABLE_TRACING
  if (pDevExt->hide)
    TraceMask((PC0C_COMMON_EXTENSION)pDevExt, "Enabled hidden mode ", codeNameTableShowPort, pDevExt->hide);
  else
    Trace0((PC0C_COMMON_EXTENSION)pDevExt, L"Disabled hidden mode");
#endif /* ENABLE_TRACING */
}
