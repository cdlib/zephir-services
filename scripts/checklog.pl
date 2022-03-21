#!/usr/bin/perl -w
#
#	checklog.pl config.file
#		The one parameter is the name of a file consists of a number of lines 
#		which control this script.
#		The lines in this file are (read in any order - but written in this
#		order):
#			logfile=<logfilename>
#			matchpattern=<pattern> to check log with.
#			exludepattern=<pattern> for things to ignore (after match)
#			lastlinechecked=<number> the lines number of the log to start.
#				Defaults to 0.
#			problemstate=<state>. This is none or problem. Defaults to none.
#			mailto=(maillist). Defaults to stdout.
#            
# Outline of routine to review log output
#
# 2) get the problemstate. If no problemstate, set to none
# 3) get lastlinechecked. If no lastlinechecked, set to 0
# 4) get the number of lines in the log file 
#    if that is less than lastlinecheched
#     clear the lastlinechecked to 0
# 5) search for problem pattern starting after the lastlinechecked.
#	 If found,
#		 if problemstate is already problem,
#			 do nothing.
#		 if problemstate is none,
#			 set problemstate = problem
#			 write report
#	 if not found,
#		 if problemstate = none,
#			 do nothing.
#		 if problemstate is problem,
#			 set problemstate = none
#			 write report
# 6) save the creationdate
# 7) save the problemstate
# 8) set lastlinechecked to the number in step 4. 
# 9) save lastlinechecked
#
# 02/11/09 - Fixed the file lengthcheck. (MJT) 
# 03/17/15 - tail +<num> changed to tail -n +<num> in support of AWS. (MJT) 
# 11/24/15 - Put To in the mail header and removed it from the mail commmand. 
#            in support of AWS. (MJT) 
# 11/25/15 - Added an extran newline at the end of each line of the result to keep
#            the mailer from runnin the lines together. (MJT) 

##########################################
# initialize and read in the config file 
##########################################

$debug = "";
$logfile="";
$lastlinechecked=0;
$problemstate="none";
$mailto="";
$matchpattern="";
$excludepattern="";
unlink (",mail_header");

if ($#ARGV < 0) {
	die "Useage: checklog.pl configfilename [debug]\n";
} 

if ($#ARGV >= 1) {
	if ("$ARGV[1]" eq "debug") {
		$debug = "yes";
	}
}

print "checklog.pl $ARGV[0]\n" if $debug;

open (CONFIG, "<$ARGV[0]") || die "Can't open file $ARGV[0]: $!\n";
    while (<CONFIG>) {
	chomp;
	if (m/^\s*#/) {
	} elsif ("$_" ne "") {
	    ($var, $val) = split (/=/, $_);	
	    if ("$var" eq "logfile") {
		$logfile = $val;
	    } elsif ("$var" eq "matchpattern") {
                $matchpattern = $val;
            } elsif ("$var" eq "excludepattern") {
                $excludepattern = $val;
	    } elsif ("$var" eq "lastlinechecked") {
		$lastlinechecked = $val;
            } elsif ("$var" eq "problemstate") {
                $problemstate = $val;
            } elsif ("$var" eq "mailto") {
                $mailto = $val;
	    } 
	}
    }
close (CONFIG);

if ($debug) {
	print "logfile=$logfile\n";
	print "matchpattern=$matchpattern\n";
        print "excludepattern=$excludepattern\n";
	print "lastlinechecked=$lastlinechecked\n";
	print "problemstate=$problemstate\n";
	print "mailto=$mailto\n";
}

# check size of logfile 
if (! -f $logfile) {
	die "Logfile $logfile from $ARGV[0] is not a regular file\n";
}
`wc -l $logfile` =~ m/\s*(\d+)\s/;
$filelength = $1;
if ($filelength < $lastlinechecked) {
	$lastlinechecked = 0;
	print "new file!\n" if $debug;
}
if ($debug) {
	print "filelength = $filelength\n";
	print "lastlinechecked = $lastlinechecked\n";
}

##########################################
# Look for the pattern
##########################################

$lastlinechecked++;
if ("$excludepattern" ne "") {
    $result = `head -$filelength $logfile | tail -n +$lastlinechecked  | egrep "$matchpattern" | egrep -v "$excludepattern"`;
} else {
    $result = `head -$filelength $logfile | tail -n +$lastlinechecked  | egrep "$matchpattern"`;
}
if ("$result" ne "") { 
	print "found?\n$result\n" if $debug; 
	if ("$problemstate" eq "none") {
		&problem_start ();
	} else {
		&problem_continue ();
	}
} elsif ("$problemstate" eq "problem") {
	&problem_end ();
} else {
	&problem_none ();
}
$lastlinechecked =  $filelength;
print "new lastlinechecked = $lastlinechecked\n" if $debug;
print "new problemstate = $problemstate\n" if $debug;
 
##########################################
# rewrite the config file`
##########################################
unlink ("$ARGV[0].temp");
open (CONFIG, "<$ARGV[0]") || die "Can't open file $ARGV[0]: $!\n";
open (NEWCONFIG, ">$ARGV[0].temp") || die "Can't open file $ARGV[0].temp: $!\n";
while (<CONFIG>) {
	chomp;
	if ("$_" eq "") {
		print NEWCONFIG "\n";
	} else {
		($var, $val) = split (/=/, $_);
		if ("$var" eq "lastlinechecked") {
			print NEWCONFIG "lastlinechecked=$lastlinechecked\n"; 
		} elsif ("$var" eq "problemstate") {
			print NEWCONFIG "problemstate=$problemstate\n";
		} else {
			print NEWCONFIG "$_\n";
		}
	}
}
close (CONFIG);
close (NEWCONFIG);
unlink ("$ARGV[0]");
rename("$ARGV[0].temp", "$ARGV[0]") || die "Couldn't rename $ARGV[0].tempto $ARGV[0]: $!\n";

#####################
# subroutines
#####################

sub problem_start 
{
	$problemstate = "problem";
	print "Problem\n" if $debug;
	open (MAIL, ">,mail_header") || die "Can't open: ";
        print MAIL "To: $mailto\n";
	print MAIL "From: checklog\n";
	print MAIL "Subject: ALERT - important messages found in $logfile\n\n";
        print MAIL "ALERT - important messages found in $logfile by the checklog script\n\n";
	print MAIL "Found:\n\n";
        my @lines = split /\n/, $result;
        foreach my $line (@lines) {
            print MAIL "$line\n\n";
        };
	close (MAIL);
	if ("$debug" eq "") {
		if ("$mailto" ne "") {
			`cat ,mail_header | /bin/mail -t`;
		}
	} else {
		if ("$mailto" ne "") {
			print "\"Problem detected\" mail not sent to $mailto due to debug option in affect\n";
		} else {
			print "\"Problem detected\" mail not sent - no mailto parm\n";
		}
	}
}

sub problem_continue
{
    print "Continued problem\n" if $debug;
	open (MAIL, ">,mail_header") || die "Can't open: ";
        print MAIL "To: $mailto\n";
	print MAIL "From: checklog\n";
	print MAIL "Subject: ALERT - important messages found in $logfile (continued problem)\n\n";
        print MAIL "ALERT - more important messages found in $logfile by the checklog script\n\n";
	print MAIL "Found:\n\n";
        my @lines = split /\n/, $result;
        foreach my $line (@lines) {
            print MAIL "$line\n\n";
        };
	close (MAIL);
	if ("$debug" eq "") {
		if ("$mailto" ne "") {
			`cat ,mail_header | /bin/mail -t`;
		}
	} else {
		if ("$mailto" ne "") {
			print "\"Problem continues\" mail not sent to $mailto due to debug option in affect\n";
		} else {
			print "\"Problem continues\" mail not sent - no mailto parm\n";
		}
	}
}

sub problem_end
{
	$problemstate = "none";
	open (MAIL, ">,mail_header") || die "Can't open: ";
        print MAIL "To: $mailto\n";
	print MAIL "From: checklog\n";
	print MAIL "Subject: ALERT RESOLVED - further messages no longer found in $logfile\n\n";
        print MAIL "ALERT RESOLVED - no further messages found in $logfile by checklog script\n\n";
	print MAIL "The previously reported problem seems to have resolved.\n"; 
	close (MAIL);
	if ("$debug" eq "") {
		if ("$mailto" ne "") {
			`cat ,mail_header | /bin/mail -t`;
		} 
	} else {
	   if ("$mailto" ne "") {
			print "\"Problem ended\" mail not sent to $mailto due to debug option in affect\n";
		} else {
			print "\"Problem ended\" mail not sent - no mailto parm\n";
		}
	}
}

sub problem_none
{
	$problemstate = "none";
	print "No problem\n" if $debug;

}

