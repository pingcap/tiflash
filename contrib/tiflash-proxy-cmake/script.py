import subprocess as sp
import shutil
import sys
prefix = "TIFLASH_PROXY "
required_symbols = [
    "__dso_handle",
    "run_raftstore_proxy_ffi",
    "_tiflash_symbolize",
    "print_raftstore_proxy_version"
]

if __name__ == '__main__':
    result = sp.run(['ldconfig', '-p'], capture_output=True)
    outputs = [i for i in result.stdout.decode().split('\n') if 'libc++.so' in i or 'libc++abi.so' in i or 'libc.so' in i or 'libm.so' in i or 'libdl.so' in i or 'librt.so' in i or 'libstdc++.so' in i or 'libunwind.so' in i or 'libgcc_s.so' in i]
    libs = [i.split()[-1] for i in outputs]

    for i in libs:
        syms_raw = sp.run(['nm', '-D', '--extern-only', '--defined-only', '--no-demangle', i], capture_output=True)
        syms = [j.split()[-1].split('@')[0] for j in syms_raw.stdout.decode().split('\n') if j]
        required_symbols.extend(syms)

    objcopy = shutil.which('llvm-objcopy') or shutil.which('objcopy')

    sp.run([objcopy, '--prefix-symbols', prefix, sys.argv[1], sys.argv[2]])
    args = [objcopy]
    for i in set(required_symbols):
        args.extend(['--redefine-sym', f"{prefix}{i}={i}"])
    args.append(sys.argv[2])
    sp.run(args)


	
