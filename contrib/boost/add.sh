#!/usr/bin/env bash

for i in $(cat modules.txt)
do
	git submodule add -f --depth=1 https://github.com/boostorg/$i 
	git config -f .gitmodules submodule.contrib/boost/$i.shallow true
done

mkdir -p numeric

pushd numeric

git submodule add -f --depth=1 https://github.com/boostorg/numeric_conversion conversion
git config -f .gitmodules submodule.contrib/boost/numeric/conversion.shallow true

git submodule add -f --depth=1 https://github.com/boostorg/interval 
git config -f .gitmodules submodule.contrib/boost/numeric/interval.shallow true

git submodule add -f --depth=1 https://github.com/boostorg/odeint
git config -f .gitmodules submodule.contrib/boost/numeric/odeint.shallow true

git submodule add -f --depth=1 https://github.com/boostorg/ublas
git config -f .gitmodules submodule.contrib/boost/numeric/ublas.shallow true

popd
