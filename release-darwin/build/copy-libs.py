#!/usr/bin/python
import os
import subprocess
import sys

copied = {}
rpath = set()
done = {}

def copyLib(path):
    baseName = os.path.basename(path)
    if baseName in copied:
        return
    subprocess.Popen(["install", "-vp", path, baseName], stdout=subprocess.PIPE).communicate()[0]
    copied[baseName] = True

def changeToLocal(path):
    baseName = os.path.basename(path)
    if path in done:
        return []
    output = subprocess.Popen(["/usr/bin/otool", "-L", path], stdout=subprocess.PIPE).communicate()[0]
    res = []
    for i, line in enumerate(output.split('\n')[1:]):
        line = line.strip()
        if line.startswith("/usr/local"):
            lib = line.split()[0]
            libBaseName = os.path.basename(lib)
            if libBaseName == baseName:
                subprocess.Popen(["install_name_tool", "-id", "@executable_path/%s" % libBaseName, baseName], stdout=subprocess.PIPE).communicate()[0]
            else:
                subprocess.Popen(["install_name_tool", "-change", lib, "@executable_path/%s" % libBaseName, baseName], stdout=subprocess.PIPE).communicate()[0]
            res.append(lib)
        if line.startswith("@rpath"):
            lib = line.split()[0]
            libBaseName = os.path.basename(lib)
            rpath.add((lib, libBaseName, baseName))
    done[baseName] = True
    return res

def run(path):
    os.chdir(os.path.dirname(path))
    libBaseName = os.path.basename(path)
    libs = changeToLocal(path)
    while len(libs) > 0 or len(rpath) > 0:
        while len(libs) > 0:
            path = libs.pop()
            libBaseName = os.path.basename(path)
            copyLib(path)
            newLibs = changeToLocal(path)
            for lib in newLibs:
                if not lib in done and not lib in libs:
                    libs.insert(0, lib)
        while len(rpath) > 0:
            lib, libBaseName, baseName = rpath.pop()
            subprocess.Popen(["install_name_tool", "-change", lib, "@executable_path/%s" % libBaseName, baseName], stdout=subprocess.PIPE).communicate()[0]

if __name__ == '__main__':
    run(sys.argv[1])
