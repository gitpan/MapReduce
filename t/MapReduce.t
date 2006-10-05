# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl MapReduce.t'

#########################

# change 'tests => 1' to 'tests => last_test_to_print';

use Test::More tests => 2;
BEGIN { use_ok('MapReduce') };

#########################

# Insert your test code below, the Test::More module is use()ed here so read
# its man page ( perldoc Test::More ) for help writing this test script.
use Cwd;
my $dir = getcwd();

system "perl bin/mr -repository=$dir/t -conf=test.conf bin/wc.mr odyssey odyssey_wc";

ok(1, "Check t/odyssey_wc/odyssey_wc.txt for result");

