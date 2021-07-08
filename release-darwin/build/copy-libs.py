#!/usr/bin/python
import os
import subprocess
import sys

copied = {}
rpath = set()
done = {}

def copyLib(path):
    #print("copy lib %s" % path)
    basePath = os.path.basename(path)
    if basePath in copied:
        return
    subprocess.Popen(["install", "-vp", path, basePath], stdout=subprocess.PIPE).communicate()[0]
    copied[path] = True

def changeToLocal(path):
    #print("change to local %s" % path)
    basePath = os.path.basename(path)
    if path in done:
        return []
    output = subprocess.Popen(["/usr/bin/otool", "-L", path], stdout=subprocess.PIPE).communicate()[0]
    res = []
    for i, line in enumerate(output.split('\n')):
        line = line.strip()
        if line.startswith("/usr/local"):
            lib = line.split()[0]
            lib = lib[:-1] if lib[-1] == ':' else lib
            #print("add lib %s" % lib)
            res.append(lib)
    for lib in res:
        libPath = os.path.basename(lib)
        print("changeToLocal: changing path of %s in %s," % (libPath, basePath))
        if libPath == basePath:
            subprocess.Popen(["install_name_tool", "-id", "@executable_path/%s" % libPath, basePath], stdout=subprocess.PIPE).communicate()[0]
        else:
            subprocess.Popen(["install_name_tool", "-change", lib, "@executable_path/%s" % libPath, basePath], stdout=subprocess.PIPE).communicate()[0]
    for i, line in enumerate(output.split('\n')):
        line = line.strip()
        if line.startswith("@rpath"):
            lib = line.split()[0]
            libPath = os.path.basename(lib)
            rpath.add((lib, libPath, basePath))
    done[path] = True
    return res

def run(path):
    os.chdir(os.path.dirname(path))
    libPath = os.path.basename(path)
    libs = changeToLocal(path)
    while len(libs) > 0 or len(rpath) > 0:
        while len(libs) > 0:
            path = libs.pop()
            libPath = os.path.basename(path)
            copyLib(path)
            newLibs = changeToLocal(path)
            for lib in newLibs:
                if not lib in done and not lib in libs:
                    libs.insert(0, lib)
        while len(rpath) > 0:
            lib, libPath, basePath = rpath.pop()
            subprocess.Popen(["install_name_tool", "-change", lib, "@executable_path/%s" % libPath, basePath], stdout=subprocess.PIPE).communicate()[0]

if __name__ == '__main__':
    run(sys.argv[1])
