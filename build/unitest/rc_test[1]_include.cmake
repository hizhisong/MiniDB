if(EXISTS "/home/tao/Desktop/miniob-contest/build/unitest/rc_test[1]_tests.cmake")
  include("/home/tao/Desktop/miniob-contest/build/unitest/rc_test[1]_tests.cmake")
else()
  add_test(rc_test_NOT_BUILT rc_test_NOT_BUILT)
endif()