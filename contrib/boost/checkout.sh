#!/usr/bin/env bash

VERSION=$1

for i in $(cat modules.txt)
do
	pushd $i
	git fetch origin $VERSION:$VERSION --depth=1
	git checkout $VERSION
	popd
done

mkdir -p numeric

pushd numeric

	for i in conversion interval odeint ublas
	do
		pushd $i
		git fetch origin $VERSION:$VERSION --depth=1
		git checkout $VERSION
		popd
	done

popd
