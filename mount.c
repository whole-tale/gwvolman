/*
 * BSD 3-Clause License
 *
 * Copyright (c) 2018, Huamin Chen
 * Copyright (c) 2018, Kacper Kowalik
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * * Neither the name of the copyright holder nor the names of its
 *   contributors may be used to endorse or promote products derived from
 *   this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */
#define _GNU_SOURCE
#define FUSE_MOUNT 0
#if FUSE_MOUNT
#define FUSE_USE_VERSION 26
#include <fuse.h>
#endif

#include <stdio.h>
#include <dlfcn.h>
#include <sys/mount.h>
#include <sched.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdint.h>
#include <errno.h>
#include <syslog.h>
#include <string.h>

/* Copied from sys-utils/nsenter.c */
static int open_target_fd(int *fd, const char *path)
{
	if (*fd >= 0)
		close(*fd);

	*fd = open(path, O_RDONLY);
	if (*fd < 0) {
        perror("open");
        return -1;
    }
    return 0;
}

int mount(const char *source, const char *target,
          const char *filesystemtype, unsigned long mountflags,
          const void *data)
{
   int (*orig_mount)(const char *, const char *,
                     const char *, unsigned long,
                     const void *);
   int targetfd = -1;

   orig_mount = dlsym(RTLD_NEXT, "mount");

   openlog ("mount.so", LOG_CONS | LOG_PID | LOG_NDELAY, LOG_LOCAL1);
   if (!open_target_fd(&targetfd, "/host/proc/1/ns/mnt")){
      if (setns(targetfd, CLONE_NEWNS)){
         syslog(LOG_NOTICE, "setns failed for filesystem: %s", filesystemtype);
      } else {
         syslog(LOG_NOTICE, "setns succeeded for filesystem: %s", filesystemtype);
      }
   } else {
      syslog(LOG_NOTICE, "failed to open ns for filesystem: %s", filesystemtype);
   }
   closelog();


   if (orig_mount) {
      return orig_mount(source, target, filesystemtype, mountflags, data);
   } else {
      return -ENOENT;
   }
}

int umount(const char *target)
{
   int (*orig_umount)(const char *target);
   int targetfd = -1;

   orig_umount = dlsym(RTLD_NEXT, "umount");
   
   openlog ("mount.so", LOG_CONS | LOG_PID | LOG_NDELAY, LOG_LOCAL1);
   if (!open_target_fd(&targetfd, "/host/proc/1/ns/mnt")){
      if (setns(targetfd, CLONE_NEWNS)){
         syslog(LOG_NOTICE, "setns failed for umount");
      } else {
         syslog(LOG_NOTICE, "setns succeeded for umount");
      }
   } else {
      syslog(LOG_NOTICE, "failed to open ns for umount");
   }
   closelog();


   if (orig_umount) {
      return orig_umount(target);
   } else {
      return -ENOENT;
   }
}
