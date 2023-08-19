// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <errno.h>
#include "syscall.h"
#include <syscall.h>

int futimens(int fd, const struct timespec times[2])
{
	int r = __syscall(SYS_utimensat, fd, 0, times, 0);
#ifdef SYS_futimesat
	if (r != -ENOSYS) return __syscall_ret(r);
	struct timeval *tv = 0, tmp[2];
	if (times) {
		int i;
		tv = tmp;
		for (i=0; i<2; i++) {
			if (times[i].tv_nsec >= 1000000000ULL) {
				if (times[i].tv_nsec == UTIME_NOW &&
				    times[1-i].tv_nsec == UTIME_NOW) {
					tv = 0;
					break;
				}
				if (times[i].tv_nsec == UTIME_OMIT)
					return __syscall_ret(-ENOSYS);
				return __syscall_ret(-EINVAL);
			}
			tmp[i].tv_sec = times[i].tv_sec;
			tmp[i].tv_usec = times[i].tv_nsec / 1000;
		}
	}

	r = __syscall(SYS_futimesat, fd, 0, tv);
	if (r != -ENOSYS || fd != AT_FDCWD) return __syscall_ret(r);
	r = __syscall(SYS_utimes, 0, tv);
#endif
	return __syscall_ret(r);
}
