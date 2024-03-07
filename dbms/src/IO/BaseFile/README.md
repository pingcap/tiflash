# BaseFile

BaseFile is a file system abstraction layer that provides a common interface for file system operations. All file system operations should be performed through BaseFile.

**Note**

> To avoid unnecessary dependencies, all classes in this directory should only depend on classes in this directory or `Common` directory.

- WritableFile: A writable file abstraction. It provides all the functions that a file system should support for writing.
- RandomAccessFile: A random access file abstraction. It provides all the functions that a file system should support for random access.
- WriteReadableFile: A writable and readable file abstraction. It provides all the functions that a file system should support for both writing and reading.
- PosixXxxFile: A file abstraction for posix file system.
- RateLimiter: Used to control read/write rate.
- fwd.h: Forward declaration of all classes in this directory. It is recommended to include this file in other header files instead of including the header files directly to avoid unnecessary dependencies.
