cd ./build
cmake -DDEBUG=ON ..
make -j8
cd ../
./build/bin/observer -f ./etc/observer.ini

