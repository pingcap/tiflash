#!/usr/bin/python
import os
import subprocess
import sys

copied = {}
rpath = set()
done = {}


def copy_lib(path):
    base_name = os.path.basename(path)
    if base_name in copied:
        return
    subprocess.Popen(["install", "-vp", path, base_name], stdout=subprocess.PIPE).communicate()
    copied[base_name] = True


def change_to_local(path):
    base_name = os.path.basename(path)
    if path in done:
        return []
    output = subprocess.Popen(["otool", "-L", path], stdout=subprocess.PIPE).communicate()[0].decode("utf8")
    res = []
    for line in output.split('\n')[1:]:
        line = line.strip()
        if line.startswith("/usr/local"):
            lib = line.split()[0]
            lib_base_name = os.path.basename(lib)
            if lib_base_name == base_name:
                subprocess.Popen(["install_name_tool", "-id", "@executable_path/%s" % lib_base_name, base_name], stdout=subprocess.PIPE).communicate()
            else:
                subprocess.Popen(["install_name_tool", "-change", lib, "@executable_path/%s" % lib_base_name, base_name], stdout=subprocess.PIPE).communicate()
                res.append(lib)
        if line.startswith("@rpath"):
            lib = line.split()[0]
            lib_base_name = os.path.basename(lib)
            rpath.add((lib, lib_base_name, base_name))
    done[base_name] = True
    return res


def run(path):
    os.chdir(os.path.dirname(path))
    libs = change_to_local(path)
    while len(libs) > 0 or len(rpath) > 0:
        while len(libs) > 0:
            path = libs.pop()
            copy_lib(path)
            new_libs = change_to_local(path)
            for lib in new_libs:
                if lib not in done and lib not in libs:
                    libs.insert(0, lib)
        while len(rpath) > 0:
            lib, lib_base_name, base_name = rpath.pop()
            subprocess.Popen(["install_name_tool", "-change", lib, "@executable_path/%s" % lib_base_name, base_name], stdout=subprocess.PIPE).communicate()


if __name__ == '__main__':
    run(sys.argv[1])
