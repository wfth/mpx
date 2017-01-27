#!/bin/perl -w
################################################################
#
#	Program :	WisdomCD MPX.pl
#
#	This program is used to read email cd orders and generate
#	CD order files to Rimage's Network Publiser
#
#	NOTE:
#	CURRENTLY, MANY OF THE OPERATING PARAMETERS ARE HARD CODED
#  ANY CHANGES SUCH AS DATABASE NAME, EMAIL ACCOUNT AND PASSWORDS
#  MUST BE CHANGED IN THE VARIABLE DEFINITIONS
#  (POSSIBLE CHANGES IS TO READ THESE PARAMETERS FROM AN INI FILE)
#
#	Requirements:
#
#  This program is written in Perl
#  email server as defined in $mailserver
#	MySQL database : wisdomsql
#  ActivePerl packages (Listed in the heading)
#		DBI; Net::POP3; Mail::Internet; Net::SMTP
#
#
#	Input from email order:
#  	Each line in the email must have this format:
#
#		CatalogID	Quantity		Title [optional]
#			CatalogID is the order items as defined in the CatalogTable of wisdomSQL
#			Title : Optional, Description of this item
#			Quantity  is the number of CD (or sets of CD) wanted.
#				This number must be between 1 and 99
#
#	Output :
#		Order ID : This ID is make up of the CD ID and a sequence number
#
#		Rimage Publisher order file with file name eq seq#.nwp.
#				This file is located by $RimageDir and will be read and
#				process by the "Rimage Network Publisher" program.
#
#  Process:
#	* open a connection to a pop3 mail server ($mailserver)
#  * if there is(are) mail in the account, process the mail
#		else wait (currently set to 2 minutes) then try to read again.
#	* Process mail
#		*	read and parse each mail
#		*	get catalogID and quantity only (currently assume to be CD orders only)
#		*	if database is not connected, connect to mysql database
#		*	search each catalog id from database, if catalog id is found
#			generate Rimage Publisher Order file.
#		* 	If item or items related to the catalogID cannot be found,
#			generate an email to $toUser notify of the error
#
#	* Manual Process
#		* User enter the Catalog ID and Quantity
#		* Process the catalog item the same as in eMail by calling the subroutine
#		  ProcessOrder
#
#	Copyright: Wisdom for the Heart
#
#	Changes :
#	11/15/2006		initial development	David Wu
#  04/01/07 version 1.40 		Add print transcript and status messages
#	05/13/07 version 1.50		Add check for duplicate orders
#  08/01/07 version 1.80      Fix error reporting
#  03/25/08 Version 2.00 		Add MS SQL Server
#  06/10/08 Version 3.01      Add transcript delay queue and email bookstore delay queue
#  06/20/08 Version 3.02		Combine CD orders with the same product code
#  06/25/07 Version 3.03      Fix error message
#
my $version = "Version 3.03";
################################################################
# import package
use warnings;

# use strict ;
use DBI;
use Net::POP3;
use Mail::Internet;
use Net::SMTP;

# print color
use Win32::Console::ANSI;
use Term::ANSIColor;
#############################################################
#############################################################
#
#  Changes between Wisdom office and home
#
#############################################################


#############################################################
#  MS SQL Server
#############################################################
#  my $MSdb   = "SQL Server";        # sql user database name
my	$MSdb   = $ENV{'MYSQL_DB'};             #  MS SQL  user database name

my	$MShost = $ENV{'MYSQL_HOST'};
my	$MSPort = $ENV{'MYSQL_PORT'};            #default port

my $MSuser = $ENV{'MYSQL_USER'};              #  MS SQL  database user name

my $MSpass =  $ENV{'MYSQL_PASSWORD'};    # "wfth2009" ;  # MS SQL database password

my $MS_DSN = "DBI:ODBC:Driver={SQL Server};Server=$MShost;port=$MSPort;Database=$MSdb";
#  my $MS_DSN = "DBI:mysql:mysql:localhost;Database=mpx";

#############################################################
#  Directories
#############################################################
#  my $RimageDir = "C:\\Publisher Orders\\";
my $RimageDir = "I:\\Rimage files\\Publisher Orders\\";

my $TranscriptDir = "D:\\Transcripts\\";
# my $TranscriptDir = "G:\\Wisdom\\";

#  my $outServer     = "smtp-server.nc.rr.com";
my $outServer = $ENV{'OUTBOUND_EMAIL_SERVER'};

# my $dummyFile = "C:\\DummyFile.pdf";    # Print this if print been idle too long
my $dummyFile = "D:\\Transcripts\\OKIDummyPage.pdf";  # Print this if print been idle too long

my $AudioDir      = "\\\\Deacon\\WFTH Data\\Rimage Files\\Wisdom Audio\\";
my $VideoDir      = "\\\\Deacon\\WFTH Data\\Rimage Files\\Wisdom Video\\";
my $ArtWorkDir    = "\\\\Deacon\\WFTH Data\\Rimage Files\\Artwork\\";

#############################################################
#  POP3 Mail Data (incoming)
#############################################################

	my $pop3;

my $username   = $ENV{'INBOUND_EMAIL_USERNAME'};    ## Production name

my $password   = $ENV{'INBOUND_EMAIL_PASSWORD'};
my $mailserver = $ENV{'INBOUND_EMAIL_SERVER'};

#	Ghostscript system call
my $GScall =
"gswin32c.exe -dNOSAFER -dNOPAUSE -dBATCH -sDEVICE=mswinpr2 -sOutputFile=\"%printer%OKI C5100\" ";

#  my $GScall = "gswin32c.exe -dNOSAFER -dNOPAUSE -dBATCH -sDEVICE=mswinpr2 -sOutputFile=\"%printer%Lexmark 3400 Series\" ";
#############################################################
#  Adobe Reader data path
#############################################################
my $AdobePath =
  "C:\\\"Program Files\"\\Adobe\\\"Acrobat 7.0\"\\Reader\\";    # my path
my $AdobeExe    = "AcroRd32.exe";
my $AdobeReader = $AdobePath . $AdobeExe;
######################################################
# SMTP - Send Mail
######################################################
my $fromRimage = $ENV{'RIMAGE_FROM_EMAIL_ADDRESS'};
my $Rob        = $ENV{'ROB_EMAIL_ADDRESS'};

my $David   = $ENV{'DAVID_EMAIL_ADDRESS'};
my $email   = $ENV{'RIMAGE_ERROR_RECIPIENT_EMAIL_ADDRESS'}; # email address for rimage send error
my @ErrorCC = ( $David, $Rob );            # Error messages

# 	my @ErrorCC = ($David) ;
#############################################################
#  Start definitions
#############################################################
#############################################################
# Global definitions
#############################################################
my $Max_Orders     = 99;
my $NormalWaitTime = 10;      # Wait time in minutes between read email
my $SQLWaitTime    = 5;       # Wait time for processing MPX Batch Records
my $DSLRetry       = 15;
my $BookStoreHold  = 1600;    # Book store hold till 1600 hour (4pm)
my $waitTime = $NormalWaitTime;
my $printDelayTime = 6 * 60 * 60;  # if we have not print anything in this hours
my $LastPrintTime  = 0;
my $sleepBatch     = 0;

# used by getOrderID subroutine
my $seqid          = 1;            # make it global so it stick
my $old_dayOfMonth = 0;
my @orderList      = ();           # List of orders received today
#############################################################
# Status counters
#############################################################
my $NoEmail      = 0;
my $NoOrders     = 0;
my $NoCDs        = 0;
my $NoError      = 0;
my $NOCDProduced = 0;
my $NoPDF        = 0;
my $NoBatchRc    = 0;
my $NoRecords    = 0;
my $box    = "************************************************************\n";
my $errMsg = "";
my @PDFQueue;    # PDF Queue
#############################################################
#	SQL DATA
#############################################################
my $db        = $ENV{'MYSQL_DB_2'};           # mysql user database name
my $user      = $ENV{'MYSQL_USER_2'};         # mysql database user name
my $pass      = $ENV{'MYSQL_PASSWORD_2'};     # mysql database password
my $host      = "localhost";                  # user hostname
my $mySQL_DSN = "\"DBI:mysql:$db:$host\" ";

# This should be "localhost" but it can be diffrent too
#############################################################
#############################################################
## MySQL DataBase Name
#############################################################
my $myDataBase = "wisdomsql";

# My Table Names
my $CatalogTable    = "CatalogTable";
my $CDTable         = "CDTable";
my $audioTable      = "AudioTable";
my $audioFileTable  = "AudioFileTable";
my $ScriptTable     = "TranscriptTable";
my $ScriptFileTable = "TranscriptFileTable";
my $MyBatches       = "myBatches";
my $ComboSeries     = "ComboSeries";

#
# Select MySQLs
#
# Prepare CatalogTable
# my $prepareCatalog =
  # "SELECT * FROM $CatalogTable WHERE catalogID = ? and type like '%C%' ";
my $prepareCatalog =
  "SELECT type FROM $CatalogTable WHERE catalogID = ? ";
# Prepare CDTable
my $prepareCD = "SELECT * FROM $CDTable WHERE catalogID = ?";

# Prepare AudioTable
my $prepareAudio = "SELECT * FROM $audioTable WHERE CDID = ? order by trackNum";

# Prepare AudioFileTable
my $prepareAduioFile = "SELECT * FROM $audioFileTable WHERE audioID = ?";

# Prepare BatchesTable
my $prepareMyBatches     = "SELECT * FROM $MyBatches WHERE BatchNbr = ?";
my $prepareInsertMyBatch =
  "INSERT INTO $MyBatches( BatchNbr, BatchStatus, ApplyDate) VALUES (?,?,?)";

######################################################
# Connect to data base
######################################################
my ($dbh);
my ( $QComboSeries, $QCatalog, $QueryCD, $QAudio, $QAudioFile );
my ( $QScript, $QScriptFile, $QMyBatches, $QInsertBatches );
#############################################################
#############################################################
## MS SQL Server DataBase Name (ManPower database)
#############################################################
my $noMPX = 0;		# Cannot connect to MPX database
my $MPXErrStr = ""; # MPX return error string
my $MSDataBase = "mpx";

# SQL Server Table Names
my $Batches     = "Batches";
my $OrderHeader = "OrderHeader";
my $OrderDetail = "OrderDetail";
my $Products    = "Products";
######################################################
# Connect to SQL Server data base
######################################################
my ($MSdbh);
my ( $QBatches, $QOrderHeader, $QOrderDetail, $QProducts );
#############################################################
#
# Rimage Data directory and file
#
#############################################################
my $TmpExt     = ".tmp";		# Temp file name use when building order file
my $RimageExt  = ".nwp";		# Rimage order file (waiting)
my $RimageProc = ".inp0";		# Rimage order file (processing)
my $anyName 	= "*";			# any file name

# Job statement :Digital="prohibited" PauseFrames="0" PreEmphasis="with"
# my $audio_parm = " Digcopy:NO, pause:0, emph:ON";
my $logFile   = $RimageDir . "CDOrder.log";     # Save log
my $errorFile = $RimageDir . "ErrorLog.log";    # Save error log
#############################################################
# Declare the subroutines
#############################################################
sub connectTable;          # connect to mySQL
sub ProcessCD;             # Process CDs
sub ProcessOrder;          # process order/ query mySQL
sub ProcessTranscript;     # process transcript/ query mySQL
sub disconnectTable;       # disconnect from mySQL
sub sendMail;              # Send smtp mail
sub Manually;              # Manual process requests
sub eMail;                 # Process request from eMail
sub getOrderID;            # create an Order ID
sub getFileID;             # create a file name
sub printStatus;           # Print current status
sub printScripts;          # Print a PDF script file
sub connectSQLTable;       # connect to SQL Server
sub disconnectSQLTable;    # disconnect from SQL Server
sub ProcessMPX;            # Process SQL Server
sub GetOrderHeader;        # Process OrderHeader
sub GetOrderDetail;        # Process OrderDetail
sub GetProducts;           # Process Products
sub CombineOrders;			# Combine CD orders
sub ReleaseOrders;			# Release CD orders
#######################
# Utility subroutines
#######################
sub trim($);
sub ltrim($);
sub rtrim($);
sub MyDateTime;            # get current time
my @months   = qw(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec);
my @weekDays = qw(Sun Mon Tue Wed Thu Fri Sat Sun);
my (
	  $second,     $minute,    $hour,
	  $dayOfMonth, $month,     $yearOffset,
	  $dayOfWeek,  $dayOfYear, $daylightSavings
);
my $MilTime;               # 24 hours mil. time

my $autoMsg = "This is an automated message from WisdomCD.\n";
my $status  = "WisdomCD Order status Notification";
my ( $emailSubj, $emailBody );
#############################################################
#
# Program starts here !!!
#
#############################################################
# Loop control switches
my $interrupted = 0;       # Ctrl-C interrupt
                           # manual process
$LastPrintTime = 0;        # Last time we print anything

#
# Signal catcher .. set interrupt
#
$SIG{INT} = sub {
	$interrupted = 1;

	#	   my ($sig) = @_;
	#		syswrite(STDOUT, "Caught SIG($sig) \n";
};

# Setup default directory
chdir $RimageDir;

#
#	Open a log file (append if exist)
#
if ( !open CDLOGFILE, ">>$logFile" )
{
	die "Cannot create Order Log file $logFile Error $!";
}

#
#	Open a log file (append if exist)
#
if ( !open ERRLOGFILE, ">>$errorFile " )
{
	die "Cannot create Error Log file $logFile Error $!";
}

# force flush .. make sure data got saved
select( ( select(CDLOGFILE),  $| = 1 )[0] );    # force auto flush
select( ( select(ERRLOGFILE), $| = 1 )[0] );    # force auto flush
my $currenttime = MyDateTime();                 # Get current time
my $starttime   = $currenttime;

# log start time
print CDLOGFILE "$box";
print CDLOGFILE "*    $currenttime - Starting wisdom CD Publisher     \n";
print CDLOGFILE "$box";
print CDLOGFILE "\n\n";
#############################################################
#  Infinite loop starts here
##############################################################
$sleepBatch = 0;    # Wait time to check Batch records
my $sleepTime = 0;
my $print     = 0;    # Print status switch
my $manual    = 0;
my $runPgm    = 1;
my $DVDSwitch = 0;		# DVD pending message

while ($runPgm)       # infinite loop
{
	$interrupted = 0;    # reset interrupt
	                     # Since all pdf are printed at the end of this routine
	     # we clear the print queue here and process the queue at the end of
	     # the loop
	@PDFQueue = ();    # Clear print pdf Queue
	if ($manual)
	{

		# Process request Manually
		Manually();
		$manual = 0;
		$print++;			# print normal screen
	}
	(
		$second,     $minute,    $hour,
		$dayOfMonth, $month,     $yearOffset,
		$dayOfWeek,  $dayOfYear, $daylightSavings
	  )
	  = localtime();
	$MilTime = $hour * 100 + $minute;    # 24hour time

	                                     # Check batch record ?
	if (( $sleepBatch <= 0 ) && ($noMPX == 0))
	{
		$sleepBatch = $SQLWaitTime * 60;    # Wait time to check Batch records
		$DVDSwitch = 0;
		ProcessMPX();              # Process MPX Batch Records

		if ( $DVDSwitch )  # if catalog type is Video
		{
			print color 'bold red';
			print "\n\tVIDEO REQUEST PENDING!!!\n" ;
			print "\tMAKE SURE TO PUT DVD(s)IN THE CD TRAY\n";
			print "\tAND RENAME FILE CODE($TmpExt) to ($RimageExt)\n\n";
			print color 'reset';
			$DVDSwitch = 0;
		}
		if ($noMPX)		# Cannot process DATAbase
		{
			print ERRLOGFILE "Cannot connect to MPX Database\n";
			print ERRLOGFILE "$MPXErrStr";
		}
		$print++;                           # Print status
	}

	# Check eMail ?
#	if ( $sleepTime <= 0 )
#	{
#		$sleepTime = $waitTime * 60;
#	#	eMail();                            # process request from eMail
#		# debug
#		ReleaseOrders();
#		$print++;                           # print status
#	}
	if ($print)                            # If we process something
	{
		printStatus(0);                     # Print status
		$print   = 0;                        # reset print switch
	}

	# OK see if we have any pdf files in the queue
	# We delay the process of pdf after all CD been processed
	foreach my $nextEntry (@PDFQueue)
	{
		my ( $pdf, $Qty ) = @$nextEntry;
		printScripts( $pdf, $Qty );         # Ok print the script file
	}

	# Have to use this loop to catach Win32 SIG(INT)
	sleep(2);                              # sleep and wait for interrupt
	$sleepTime  -= 2;
	$sleepBatch -= 2;
	if ($interrupted)
	{
		$interrupted = 0;                   # reset interrupt switch
		print "\nDo you want to process CD requests manually [y/n] > ";

#  "\nPlease enter commands[C/T/O/Q]\nC = CD\nT = Transcript\nO = Print Orders\nQ = Terminate Program\n";
		$_ = <STDIN>;
		if ( defined($_) )                  # make sure no multiple ctrl-C
		{
			chomp($_);
			if (/y/i)                        # case-insensitive match
			{
				$manual    = 1;               # set manual input switch
				$sleepTime = 0;               # exit sleep loop
			}
			else
			{
				print "\nDo you want to terminate this program [y/n] > ";
				$_ = <STDIN>;
				if ( defined($_) )            # make sure no multiple ctrl-C
				{
					chomp($_);
					if (/y/i)                  # case-insensitive match
					{
						$runPgm = 0;
					}
					else
					{

						# Let it sleep ....
						$manual = 0;
						print "\nKeep waiting ...\n";
					}
				}
			}
		}
	}    # End Interrupt
#	if ( $hour >= 21 )    # check if after 9pm
#	{
#		print "After 9pm shutdown program\n ";    # print ...
#		$runPgm = 0;                              #terminate program
#	}
}    # end of infinite loop
$currenttime = MyDateTime();
print CDLOGFILE "\n$currenttime ... Terminating Wisdom CD Publisher\n\n";
print "\n$currenttime ... Terminating Wisdom CD Publisher\n";
close(CDLOGFILE);     # close the log file handle
close(ERRLOGFILE);    # close the log file handle
exit(1);
#####################################################
# Print status
#####################################################
sub printStatus
{
	my $notification = $_[0];
	my (
		  $EmailNO,      $OrdersNo,      $CDsNO,
		  $CDProducedNo, $NoTranscripts, $ErrorNo,
		  $Boottime,     $NoBatchNbr,    $NoRecordNbr
	);
	my $returnstatus;
	$Boottime   = sprintf "\nProgram start time : $starttime\n";
	$NoBatchNbr =
	  sprintf "\tTotal number of Batch Records  .......  $NoBatchRc\n";
	$NoRecordNbr =
	  sprintf "\tTotal number of Order Records  .......  $NoRecords\n";
	$EmailNO  = sprintf "\tTotal number of eMail received .......  $NoEmail\n";
	$OrdersNo = sprintf "\tTotal number of order received .......  $NoOrders\n";
	$CDsNO    = sprintf "\tTotal number of CD/Titles requested ..  $NoCDs\n";
	$CDProducedNo =
	  sprintf "\tTotal number of CD produced  .........  $NOCDProduced\n";
	$NoTranscripts =
	  sprintf "\tTotal number of transcripts printed ... $NoPDF\n";
	$ErrorNo = sprintf "\tTotal number of errors ................ $NoError\n";

	if ( $notification == 1 )
	{
		$returnstatus = sprintf
"$Boottime $NoBatchNbr $NoRecordNbr $EmailNO $OrdersNo $CDsNO $CDProducedNo $NoTranscripts $ErrorNo";
		return $returnstatus;
	}
	if ( $NoError == 0 )
	{
		print color 'dark bold blue';

		#		system("cls");    # clear screen
		print "$box";
		print "*                                                          *\n";
		print "*                  Wisdom For the Heart                    *\n";
		print "*                     MPX CD Publisher                     *\n";
		print "*                      $version                        *\n";
		print "*                                                          *\n";
		print "*                                                          *\n";
		print "*           CopyRight(c) 2006 Wisdom for the Heart         *\n";
		print "*                   www.wisdomonline.org                   *\n";
		print "*                                                          *\n";
		print "$box";
		print "\n\n";
	}
	print color 'reset';
	print color 'bold cyan';
	print "\nProgram Start Time: $starttime\n\n";
	$currenttime = MyDateTime();    #	Current time
	print "Current Time: $currenttime \n";
	print "\n\n";
	print
"$NoBatchNbr $NoRecordNbr $EmailNO $OrdersNo $CDsNO $CDProducedNo $NoTranscripts";
	print "\n";
	print color 'reset';

	if ( $NoError > 0 )
	{
		print color 'bold red';
	}
	else
	{
		print color 'bold green';
	}
	print "\tTotal number of errors ................ $NoError\n";
	print color 'bold yellow';
#	print "\nWait $waitTime minutes before checking email at $username ....\n";
	print "Wait $SQLWaitTime minutes before checking MPX Batch Records ....\n";
	print "\t(Use Ctrl-C to breakout this loop!)\n\n";
	if ($noMPX)		# Cannot connect to MPX
	{
		print color 'bold red';
		print "\nCANNOT CONNECT TO MPX DATABASE!!!\n" ;
		print "$MPXErrStr\n\n";
		print "Use Manual process only\n";
	}
	print color 'reset';
}
#####################################################
# Process requests from eMail
#####################################################
my ( $subject, $from, $hdstatus );
my ( $reply, $currentError );

sub eMail
{

	my $emailMsg;
	my $errorMsg = "";    # return error message
	my $OrgMsg   =
	  "---------------------- Original Message ----------------------\n";
	my $PlainText  = 1;
	my $IgnoreBody = 0;
	my $HoldSW;

	#
	# Connect to my pop3 mail server
	#
	$pop3 = undef;
	$pop3 = Net::POP3->new($mailserver);
	if ( !defined $pop3 )
	{
		$currenttime = MyDateTime();
		$NoError++;
		print color 'bold red';
		print "Failed to connect to $mailserver .... retry\n";
		print color 'reset';
		$errMsg = "$currenttime ... Failed to connect to $mailserver\n";
		print CDLOGFILE "$errMsg";

		#		print ERRLOGFILE "$errMsg";
		$waitTime = $DSLRetry;    # reset wait time
		return;
	}
	$waitTime = $NormalWaitTime;    # Everything back to normal wait time
	my $tot_msg = $pop3->login( $username, $password );
	if ( !defined $tot_msg )
	{
		$errMsg = "Failed to authenticate $username";
		print CDLOGFILE "$errMsg";

		#		print ERRLOGFILE "$errMsg";
		exit;
	}

	#
	# Connect to MySQL
	#
	if ( !defined $dbh )    # if not already connected, call connectTable
	{
		my $rc = connectTable();    # call to connect mySQL
		if ( $rc ne "" )
		{

			# close POP3 connection
			$pop3->quit();
			$errMsg = "Cannot connect wisdom SQL tables rc = $rc \n";
			print CDLOGFILE "$errMsg";

			#			print ERRLOGFILE "$errMsg";
			return;
		}
	}
	####################################################################
	#
	#  The following works with none MIME Mail ONLY
	#
	####################################################################
	foreach my $msg_id ( 1 .. $tot_msg )
	{
		$IgnoreBody = 0;    #Initial duplicate order switch
		$HoldSW     = 0;
		$NoEmail++;         # Increment no. of email received
		$reply        = "";              # reset reply
		$currentError = 0;
		$currenttime  = MyDateTime();    # call my local time routine
		print $currenttime;
		print "\n\tProcessing orders from $username\n\n";
		my $header = $pop3->top( $msg_id, 0 );
		( $subject, $from, $hdstatus ) = analyze_header($header);
		print CDLOGFILE "$currenttime ... Processing email:\n";
		print CDLOGFILE "\n\n$OrgMsg\n";
		print CDLOGFILE "From: $from\nSubject: $subject\n\n";    # Log it
		my $msg     = $pop3->get($msg_id);         # Get the whole message
		my $MailObj = Mail::Internet->new($msg);
		my ( $Catalog_ID, $Quantity, $count, $Title, $Type, @Line );
		$count     = 1;
		$PlainText = 1;                            # Assume plain text message
		$emailMsg  = "";

		# Save this message in the log file
		my @body = @{ $MailObj->tidy_body() };

		# create a reply message
		$emailBody =
		  sprintf( "%s\n> From: %s\n> Subject: %s\n\n" . ( "> %s" x @body ),
					  $OrgMsg, $from, $subject, @body );

  #			print "Debug .............\n$emailBody ............\n";     #  <---- debug
		print CDLOGFILE
		  "@body\n************ end of message ********************\n\n";
		###################################################################
		# Check for status
		###################################################################
		# Check for status request
		if ( $subject =~ /status/i )
		{
			print "Receive status request\n";
			$emailMsg     = printStatus(1);
			$currentError = -1;               # For status we are using count = -1;
			$IgnoreBody   = 1;                # looking for status, end of message
		}
		###################################################################
		# Check for store order
		###################################################################
		# check if it is a eMail order
		if ( $subject =~ /bookstore/i )
		{

			# See if we should release the eMail delay queue
			# $MilTime was set at the main wait loop
			if ( $BookStoreHold > $MilTime )    # bookstore hold?
			{
				$HoldSW     = 1;                 # Set eMail request queue on
				$IgnoreBody = 1;
				print "Ignore bookstore order for now\n";
			}
		}
		###################################################################
		# Check for hold order
		###################################################################
		if ( $subject =~ /hold till [0-9]+/i )    # If hold order
		{
			my @thisSubj = split( /\s+/, trim($subject) );    # strip blanks
			my $holdtime =
			  pop(@thisSubj);    # get the hold time (should be the last word)
			print "Received hold till $holdtime hours\n";

			# $MilTime was set at the main wait loop
			if ( $holdtime > $MilTime )    # not yet hold time
			{
				print "Hold this email\n";
				$HoldSW     = 1;            # Hold this email
				$IgnoreBody = 1;            # ignore this email for now
			}
		}
		if ( $IgnoreBody == 0 )           # If No duplication
		{
		 BODYLOOP: foreach my $body_line (@body)    # get the body of message
			{
				$Catalog_ID = "";                     # initize each local variable
				$Title      = "";
				$Quantity   = "";

				#	print "Line $count \n";			#### Debug
				#	print $body_line;					#### Debug
				$body_line = trim($body_line);    # strip leading and trading blanks
				if ( $body_line ne "" )
				{
					@Line = split( /\s+/, $body_line );
					$_    = $body_line;

					# See if this is a MIME mail
					if ( $body_line =~ /MIME format/ )
					{

						# return with error message
						$emailSubj =
						  "CD Publisher ERROR - Cannot process HTML/MIME mail\n";
						$emailBody =
						  sprintf(
"Currently we cannot process multi-part message in HTML/MIME format\n\nPlease resend the order in PLAIN TEXT format\n%s",
							$reply );
						print CDLOGFILE "$emailSubj $emailBody";   # log error
						print "$emailSubj $emailBody";             # show error
						$currentError++;                           # This order is bad
						$NoError++;    # increment total error counters
						last BODYLOOP
						  ;    # exit this message body loop, get another message
					}
					$Catalog_ID = shift(@Line);    # get first word
					$Quantity   = pop(@Line);      # Get last word as quantity
					$Type       =
					  shift(@Line); # get the second word as type of order (C, T etc)

					#	$Title   = join " ",@Line ;    # get the rest as title
					print "Catalog ID is $Catalog_ID\n";    #### Debug
					    #  print "Quantity is $Quantity \n" ;			#### Debug
					    #	print "Title is $Title \n";					#### Debug
					chomp( $_ = $Catalog_ID );
					if (/status/i)
					{
						print "Receive status request\n";
						$emailMsg = printStatus(1);
						$currentError = -1;    # For status we are using count = -1;
						last BODYLOOP;         # looking for status, end of message
					}

		# Now check if this is formated by WisdomOnLine web site
		# If from WisdomOnLine uses HTML multipart, so we just ignore HTML section
		# Look for " --=" ; "Content-Transfer-Encoding: " "Content-Type: "
					if (    (/--=/)
						  || (/Content-Transfer-Encoding:/)
						  || (/Content-Type:/) )
					{

						# Ignore this line
						# print "Ignore this line\n\t$body_line\n";    # debug
						if ( (/Content-Type:/) && ( $Type =~ /html/ ) )
						{

			  # Found a html section, set ignore all lines
			  #							print "Found a html section, set ignore all lines\n";	# debug
							$PlainText = 0;    # from now on, ignore lines
						}
						if ( (/Content-Type:/) && ( $Type =~ /plain/ ) )
						{

							# found Plain text section, keep lines
							$PlainText = 1;
						}
					}
					else
					{
						if ( $PlainText == 0 )    # Not Plain Text mode ignore line
						{

							# We must be reading HTML section
							# print "Ignore HTML line ...\n\t$body_line\n";    # debug
						}
						else
						{

							#  Make sure we have a CatalogID or Quantity
							print "Current line = $body_line\n";
							if (    ( $Catalog_ID =~ m/\w+/ )
								  && ( $Quantity =~ m/\d+/ ) )

							  #  && ( $Quantity <= $Max_Orders ) )
							{
								print "Quantity = $Quantity\n";    #debug

								# Get an orderID for this request
								# $orderID =  getOrderID();
								if ( $Type =~ /C/i )
								{
									print
"Processing CD: CatalogID $Catalog_ID Quantity $Quantity\n\n";
									$errorMsg = ProcessOrder( $Catalog_ID, $Quantity );

								}
								else
								{
									if ( $Type =~ /T/i )
									{
										print
"Process Transcript: CatalogID $Catalog_ID Quantity $Quantity\n\n";
										$emailMsg = $emailMsg
										  . "Process Transcript: CatalogID $Catalog_ID Quantity $Quantity\n";
										$errorMsg =
										  ProcessTranscript( $Catalog_ID, $Quantity );
									}
									else
									{
										$errorMsg = "Invalid Order Type $Type\n";
									}
								}
								if ( $errorMsg ne "" )
								{

									# return with error message
									$NoError++;    # increment error counters
									$currentError++;
									$emailBody = $errorMsg . "\n";      #  . $emailBody;
									print CDLOGFILE "$emailBody";    # log error
									print ERRLOGFILE "$emailBody";
									print "$emailBody";              # show error
								}
							}
							else
							{

								# Bad Catalog ID or Quanity
								$emailBody = ">\t"
								  . $body_line . "\n"
								  . "Invalid Catalog ID ($Catalog_ID) or Quantity ($Quantity) \n"
								  ;                                 # . $emailBody;
								$currentError++;
								print CDLOGFILE "$emailBody";       # log error
								                       #	print ERRLOGFILE "$emailBody";
								print "$emailBody";    # show error
							}    # End of bad catalog & quantity
						}    # End of ignoreLines
					}    # End ignore line
				}    # End of this blank line
				$count++;
			}    # End of this message
		}    # End of duplication order

		# end of this message
		if ( $currentError > 0 )
		{
			$emailSubj = $status . " : ERROR\n";
			$emailBody = $autoMsg
			  . "Sorry, we found one or more error(s) when processing your order.\n"
			  . "Please correct the error listed below and resubmit the item.  Thanks\n\n"
			  . $emailBody;
		}
		else
		{
			$emailSubj = $status . "\n";
			$emailBody = $autoMsg

		  # . "Your order has been submitted to Rimage system for processing.\n\n"
			  . $emailMsg;
		}
		if ( $currentError != 0 )    # send email only on error or status
		{

			# send an email out
			sendMail( $from, $emailSubj, $emailBody, $currentError );
		}
		if ($HoldSW)                 #if hold email order
		{
			print "Do not delete this email\n";

			#			print "HoldSW $HoldSW \n"
		}
		else
		{

			# Mark delete: not really deleted until quit is called
			###!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			$pop3->delete($msg_id);
			print "Delete msg # $msg_id \n";
		}
		print "\n=================\n\n";
	}    # End of this message

	# close POP3 mail connection
	$pop3->quit();

	# Disconnect SQL
	disconnectTable();
	print "\n";
}    # end of eMail
#####################################################
# Process requests Manually
#####################################################
sub Manually
{
	my $errorMsg = "";

	# Search CatalogID from CDTable
	my $enter = "Enter Catalog ID [q to quit] >";
	print "\n\n";
	while ($manual)
	{
		print $enter;    # Enter Catalog ID
		$_ = <STDIN>;
		if ( defined($_) )    # make sure no multiple ctrl-C
		{
			chomp($_);
			if (($_ eq "q")|($_ eq "Q"))         # case-insensitive match
			{
				print "exit manually request loop\n\n";

				# Disconnect SQL
				disconnectTable();
				$manual = 0;    # reset manually input
				return;
			}

			# else
			my $CATID = $_;
			print "Enter quantity > ";
			my $quantity = <STDIN>;
			chomp $quantity;
			print "Is this a CD, MP3(Data) or Video request? Enter [C D or V]\n";
			$_ = <STDIN>;
			my $Type = $_;
			chomp $Type;

			if ( !defined $dbh )
			{
				my $rc = connectTable();
				if ( $rc ne "" )
				{
					print "Cannot connect table rc = $rc \n";
					exit -1;
				}
			}
			if ((/c/i) || (/D/i) || (/V/i))    # If CD,MP3 or Video request
			{
				$errorMsg = ProcessOrder( $CATID, $quantity );
				if ($DVDSwitch)	# if processed DVD
				{
					print color 'bold red';
					print "\n\tVIDEO REQUEST PENDING!!!\n" ;
					print "\tPLEASE PUT DVD(s)IN THE CD TRAY AND\n";
					print "\tPRESS ANY KEY TO CONTINUE ..... \n\n";
					print color 'reset';
					$_ = <STDIN>;

					# rename the working file to rimage file

					rename $RimageDir . $anyName . $TmpExt, $RimageDir . $anyName . $RimageExt ;
					$DVDSwitch = 0;
				}
			}
			else
			{
				if (/t/i)
				{
					$errorMsg = ProcessTranscript( $CATID, $quantity );
				}
				else
				{
					$errorMsg = "\nMust enter C, D, V or T\n";
				}
			}
			if ( $errorMsg ne "" )
			{
				$NoError++;
				print
				  "Error return from ProcessOrder/ProcessTranscript .. $errorMsg\n";
				print "Sorry! Cannot process this request\n";
			}
			print "\n\n";
		}
	}
	print "\n";
	return;
}
############################ End of Main Loop ##############################################
#####################################################
# Generate an order file ID
#####################################################
sub getFileID
{
	my $CDID      = shift;                                 #  get the input
	my $FileID    = $CDID;
	my $seq       = 0;                                     #  sequence count
	my $fileName1 = $RimageDir . $FileID . $RimageExt;     # Build a file name
	my $fileName2 = $RimageDir . $FileID . $RimageProc;    # Being processing
	my $fileName3 = $RimageDir . $FileID . $TmpExt;        # Bing build
	# Test if file exist (-e existence test function)
	while ( ( -e $fileName1 ) || ( -e $fileName2 ) || ( -e $fileName3 ) )
	{

		# This file already exist
		$seq++;
		$FileID    = $CDID . "_" . $seq;
		$fileName1 = $RimageDir . $FileID . $RimageExt;
		$fileName2 = $RimageDir . $FileID . $RimageProc;
		$fileName3 = $RimageDir . $FileID . $TmpExt;
	}
	return ($FileID);
}    # end of getOrder
#####################################################
# Generate an order ID
#####################################################
sub getOrderID
{

	#  get current time
	my (
		  $second,    $minute, $hour,       $dayOfMonth,
		  $month,     $year,   $yearOffset, $dayOfWeek,
		  $dayOfYear, $daylightSavings
	  )
	  = localtime();

	# $old_dayOfMonth and $seqid are defined in the global section so they stick
	if ( $old_dayOfMonth != $dayOfMonth )
	{
		$seqid = 1;    # reset sequence count on the new day
	}
	$old_dayOfMonth = $dayOfMonth;
	my $seq = sprintf "%02d%02d%02d", $month + 1, $dayOfMonth, $seqid;
	$seqid++;         #increment order sequence count;
	return ( "CD" . $seq );
}    # end of getOrder
#####################################################
# Generate printable time and date
#####################################################
sub MyDateTime
{
	############################################
	#	This function recturn the MyDateTime value used by
	#  SQL  YYYY-MM-DD HH:MM:SS
	############################################
	(
		$second,     $minute,    $hour,
		$dayOfMonth, $month,     $yearOffset,
		$dayOfWeek,  $dayOfYear, $daylightSavings
	  )
	  = localtime();
	$year = 1900 + $yearOffset;
	$month++;    #  Note: 0 index

#	Format : hh:mm:ss Weekday Month day Year
#  Note: 0 index
#	my $theTime = "$hour:$minute:$second, $weekDays[$dayOfWeek] $months[$month] $dayOfMonth, $year";
#SQL datetime format YYYY-MM-DD HH:MM:SS
	my $theTime = "$year-$month-$dayOfMonth $hour:$minute:$second";
	return $theTime;
}
#####################################################
# 	Process CD orders but first check for Combo series
#####################################################
sub ProcessOrder
{

	# Prepare ComboSeries
	my $prepareCombo = "SELECT * FROM $ComboSeries WHERE catalogID = ? ";

	# Get calling parameters
	my $CATName = $_[0];    # looking for this CatalogID
	my $qty     = $_[1];    # quantity of this order
	my ( $Series, $cat, $title, $seq, $rc, $rc_all );

	#
	# See if this order is a combo series
	#
	if ( !defined $QComboSeries )    # if first time since connection
	{
		$QComboSeries = $dbh->prepare($prepareCombo)
		  or die "Can't prepare SELECT $ComboSeries: $dbh->errstrn";

		#		print "Prepare QComboseries\n";
	}
	$QComboSeries->execute($CATName)
	  or die "can't execute the query: $ComboSeries->errstr";
	$rc_all = "";
	if ( $QComboSeries->rows != 0 )
	{
		my $ComboRef = $QComboSeries->fetchall_arrayref();
		$QComboSeries->finish;
		$QComboSeries = undef;
		foreach my $ComboRow (@$ComboRef)
		{
			$rc = "";
			( $cat, $Series, $title, $seq ) = @$ComboRow;
			print "Found Combo Series\n\t$cat, $Series, $title\n";
			$rc = ProcessCD( $Series, $qty );
			$rc_all = $rc_all . $rc;
		}
	}
	else
	{
		$QComboSeries->finish;
		$QComboSeries = undef;
		$rc_all = ProcessCD( $CATName, $qty );
	}
	if ( $rc_all ne "" )
	{
		print "ProcessOrder:Cannot process $CATName:\n\t$rc_all\n";
	}
	return $rc_all;
}
#####################################################
# 	Process CD order by query mySQL Tables
#####################################################


sub ProcessCD
{
	my ( $order, $CD_ID, $title, $labelFile, $trackNum, $PDF_ID );
	my ( $SermonTitle, $Scripture );
	my ( $CATID, $CatType, $quantity );
	my $fileName;
	my $rc = "";
	$NoOrders++;    # increment no of orders

	# Get calling parameters
	$CATID    = $_[0];    # looking for this CatalogID
	$quantity = $_[1];    # quantity of this order

	# search for CatalogID from CDTable
	# See if we see this item in the CatalogTable
	print CDLOGFILE
	  "ProcessOrder: Looking for CatalogID $CATID in $CDTable\n";    #### Debug

	#
	# Check with CatalogTable, make sure this item is defined
	#
	if ( !defined $QCatalog )    # if first time since connection
	{

		#		print "Line: ", __LINE__, "\n";
		$QCatalog = $dbh->prepare($prepareCatalog)
		  or die "Can't prepare SELECT $CatalogTable: $dbh->errstrn";
	}

	#		print "Line: ", __LINE__, "\n";
	$QCatalog->execute($CATID)
	  or die "can't execute the query: $QCatalog->errstr";
	if ( $QCatalog->rows == 0 )
	{

		#		print "Line: ", __LINE__, "\n";
		$QCatalog->finish;
		$QCatalog = undef;
		$errMsg   = $errMsg . "Cannot find $CATID in $CatalogTable\n";
		return $errMsg;
	}
	else
	{
		$CatType = $QCatalog->fetchrow_array();
		if (( $CatType =~ /C/i ) || ( $CatType =~ /D/i )|| ( $CatType =~ /V/i )) # CD, mp3 data,Video data
		{
			# done with this table
			$QCatalog->finish;
			$QCatalog = undef;
		}
		else
		{
			#		print "Line: ", __LINE__, "\n";
			$QCatalog->finish;
			$QCatalog = undef;
			$errMsg   = $errMsg . "Cannot find $CATID in $CatalogTable\n";
			return $errMsg;
		}
	}

	# Good found it in CatalogTable
	# Now see if we found CD id for this item
	if ( !defined $QueryCD )    # if first time since connection
	{
		$QueryCD = $dbh->prepare($prepareCD)
		  or die "Can't prepare SELECT $CDTable: $dbh->errstrn";
	}
	$QueryCD->execute($CATID)
	  or die "can't execute the query: $QueryCD->errstr";

	#
	# Search the CD_Table for CatalogID wanted
	#
	if ( $QueryCD->rows != 0 )
	{

		#	      print "Find CatalogID $CATID\n";													#### Debug
		while ( @row = $QueryCD->fetchrow_array() )
		{

			#
			# find the CD_ID(s) for this CatalogID
			#
			$CD_ID = $row[0];
			chomp( $lableFile = $row[2] );
			$lableFile = $ArtWorkDir . trim($lableFile);   # remove trailing blanks
			#############################################################
			# Create a Rimage Order file
			#############################################################
			my $order_id = getFileID($CD_ID);              # Get a file name

			# use a temp file so that Rimage will not access it while I'm writing
			$fileName = $RimageDir . $order_id . $TmpExt;
			if ( !open ORDERFILE, "> $fileName" )
			{
				$rc = "Cannot create Rimage Order file $fileName Error $!";
				return $rc;
			}
			print "order_id = $order_id\n";   # Rimage order_ID;
			print ORDERFILE
			  "order_id = $order_id\n";       # Rimage order_ID;   <===============
#			print ORDERFILE
#			  "email = $email\n";    			# Rimage email notification <=========
			$NoCDs++;
			print "copies = $quantity \n";
			print ORDERFILE
			  "copies = $quantity\n";    # Rimage quantity of copies to make

			# print  "CatType = $CatType\n";
			if ( $CatType =~ /C/i )  # if catalog type is audio CD
			{
				print ORDERFILE "disc_format = RED_BOOK\n";    # Rimage disc format
			}
			else	#MP3 (DATA) or Video files
			{
				if ( $CatType =~ /V/i )  # If video data
				{
					print ORDERFILE "media = DVDR\n";
				}
				# Data file no disc_format needed
				print ORDERFILE "Volume = Wisdom for the heart\n";    # Disc volume name
				print ORDERFILE "filetype = PARENT\n";    # File type

			}
			print "CD_ID: $CD_ID \n";
			print "\tlabel = $lableFile\n";                # Rimage label
			print ORDERFILE "label = $lableFile\n";        # Rimage label

			#
			# now look for the Audio, Video or MP3 data wanted
			#
			if ( !defined $QAudio )    # if first time since connection
			{
				$QAudio = $dbh->prepare($prepareAudio)
				  or die "Can't prepare SELECT $audioTable: $dbh->errstrn";
			}
			$QAudio->execute($CD_ID)
			  or die "can't execute the query: $QAudio->errstr";
			if ( $QAudio->rows != 0 )
			{
				while ( @row = $QAudio->fetchrow_array() )
				{
					my $track = 0;

			  # 	print "audio table 0=$row[0] 1=$row[1] 2=$row[2]\n";				#### Debug
			  #
			  # find the audioID(s) for this CD_ID
			  #
					$audioID  = $row[0];
					$trackNum = $row[2];

#	               print "\tCD_ID: $CD_ID audioID $audioID, trackNum $trackNum\n";	#### Debug
#
# now look for the AudioFile wanted
#
					if ( !defined $QAudioFile )    # if first time since connection
					{
						$QAudioFile = $dbh->prepare($prepareAduioFile)
						  or die
						  "Can't prepare SELECT $audioFileTable: $dbh->errstrn";
					}
					$QAudioFile->execute($audioID)
					  or die "can't execute the query: $audioFileTable->errstr";
					if ( $QAudioFile->rows != 0 )
					{
						while ( @row = $QAudioFile->fetchrow_array() )
						{
	  						# 	print "audioFile 0=$row[0] 1=$row[1] 2=$row[2] 3=$row[3]\n";
							my $dataFile = $row[3];
							chomp $dataFile;
							if ( $CatType =~ /C/i )  # if catalog type is audio CD
							{
								# set top dir to Wisdom Audio
								$dataFile = $AudioDir . trim($dataFile);  # remove blanks
								print "\tAudioID: $audioID, Sermon Title: $row[1]\n";
								print "Audio file \taudio_file=$dataFile\n";    # Rimage audio file
								print ORDERFILE
							  		"audio_file = $dataFile\n";        # Rimage audio file
	    					}
							else
							{
								if ( $CatType =~ /V/i )  	# If Video file
								{
									# set top dir to Wisdom Video
									$dataFile = $VideoDir . trim($dataFile);  # remove blanks
								}
								if ( $CatType =~ /D/i )  # if catalog type is MP3 audio data
								{
									# set top dir to Wisdom Audio
									$dataFile = $AudioDir . trim($dataFile);  # remove blanks
								}
								# MP3 or Video files
								print "\tAudioID: $audioID, Sermon Title: $row[1]\n";
								print "data file \tfile=$dataFile\n";    # Rimage data file
								print ORDERFILE
							  		"file = $dataFile\n";        # Rimage data file

							}
							# end of AudioFileTable loop
						}
					}
					else
					{
						#  No audio file found in AudioFileTable
						$rc = "Cannot find audioID $audioID from $audioFileTable \n" ;
 						print $rc;
					}
					$QAudioFile->finish;
					$QAudioFile = undef;
					$track++;
				}    # End of Audio Table query
			}
			else
			{

				# No entry in Audio Table
				$rc = "Cannot find audioID for CD_ID $CD_ID \n";
				print $rc;    #### Debug
			}
			$QAudio->finish;
			$QAudio = undef;
			#############################################################
			# Close Rimage Order file
			#############################################################
			# print "before close file\n";
			close ORDERFILE;
			if ( $rc eq "" )    # No error
			{

				# increment no of CD requested
				$NOCDProduced += $quantity;
				if ( $CatType =~ /V/i )  # if catalog type is Video
				{
					# Wait, need manual intervention
					$DVDSwitch ++ ;
				}
				else # assume CD or MP3 data start Process
				{
					# rename the working file to rimage file
					rename $fileName, $RimageDir . $order_id . $RimageExt;
				}

			}
			print "\n";
		}    # end of while loop in CD table
	}
	else
	{       # no entry in the CD Table
		$rc = "Cannot find CatalogID $CATID \n";
	}
	$QueryCD->finish;
	$QueryCD = undef;

	#		print "Before return: Error msg is = $errMsg (?)\n";
	return $rc;
}    # end of SearchTable subroutine
#####################################################
# 	Process Transcript order by query mySQL Tables
#####################################################
sub ProcessTranscript
{
	my ( $scriptID, $PDFfile, $SermonTitle );
	my ( $CATID, $quantity );
	my $errMsg = "";
	my $fileName;
	my $elapsedTime;

	#
	# Select SQLs
	#
	# Prepare CatalogTable
	my $prepareCatalog =
	  "SELECT * FROM $CatalogTable WHERE catalogID = ? and type like '%T%' ";

	# Prepare ScriptTable
	my $prepareScript = "SELECT * FROM $ScriptTable WHERE catalogID = ?";

	# Prepare ScriptFileTable
	my $prepareScriptFile = "SELECT * FROM $ScriptFileTable WHERE scriptID = ?";
	$NoOrders++;    # increment no of orders

	# Get calling parameters
	$CATID    = $_[0];    # looking for this CatalogID
	$quantity = $_[1];    # quantity of this order

	# search for CatalogID from CDTable
	# See if we see this item in the CatalogTable
	print "Looking for CatalogID $CATID in $CDTable\n";    #### Debug

	#
	# Check with CatalogTable, make sure this item is defined
	#
	if ( !defined $QCatalog )    # if first time since connection
	{
		$QCatalog = $dbh->prepare($prepareCatalog)
		  or die "Can't prepare SELECT $CatalogTable: $dbh->errstrn";
	}
	$QCatalog->execute($CATID)
	  or die "can't execute the query: $QCatalog->errstr";
	if ( $QCatalog->rows == 0 )
	{
		$QCatalog->finish;
		$QCatalog = undef;
		$errMsg   = "Cannot find $CATID in $CatalogTable\n";
		return $errMsg;
	}
	else
	{

		# done with this table
		$QCatalog->finish;
		$QCatalog = undef;
	}

	# Good found it in CatalogTable
	# Now see if we found Script PDF id for this item
	if ( !defined $QScript )    # if first time since connection
	{
		$QScript = $dbh->prepare($prepareScript)
		  or die "Can't prepare SELECT $ScriptTable: $dbh->errstrn";
	}
	$QScript->execute($CATID)
	  or die "can't execute the query: $QScript->errstr";

	#
	# Search the CD_Table for CatalogID wanted
	#
	if ( $QScript->rows != 0 )
	{
		$errMsg = "";    # found at least one record

		#	      print "Find CatalogID $CATID\n";													#### Debug
		while ( @row = $QScript->fetchrow_array() )
		{

			#
			# find the Script(s) for this CatalogID
			#
			$CATID    = $row[0];    # Catalog ID
			$scriptID = $row[1];    # script ID
			$NoPDF += $quantity;
			print "Catalog ID: $CATID, Script_ID: $scriptID \n";    #debug
			                                                        #
			    # now look for the PDF file wanted
			    #

			if ( !defined $QScriptFile )    # if first time since connection
			{
				$QScriptFile = $dbh->prepare($prepareScriptFile)
				  or die "Can't prepare SELECT $ScriptFileTable: $dbh->errstrn";
			}
			$QScriptFile->execute($scriptID)
			  or die "can't execute the query: $QScriptFile->errstr";
			if ( $QScriptFile->rows != 0 )
			{
				while ( @row = $QScriptFile->fetchrow_array() )
				{
					my $track = 0;

		  # print "ScriptFileTable 0=$row[0] 1=$row[1] 2=$row[2]\n";				#### Debug
		  #
		  # find the scriptID(s) for this pdf
		  #
					$scriptID    = $row[0];    # PDF Script ID
					$PDFfile     = $row[1];    # PDF file name
					$SermonTitle = $row[2];    # Sermon Title
					  # $PDFfile = $TranscriptDir . "\"" . $PDFfile ."\"" ;		# Combine it with the directory path
					$PDFfile =
					  $TranscriptDir . $PDFfile; # Combine it with the directory path
					print "\tScriptID $scriptID PDFfile $PDFfile\n";    #### Debug

					# Now call GhostScript to print the file
					# We delay the process of pdf after all CD been processed
					# This is done at the end of delay loop
					push( @PDFQueue, [ $PDFfile, $quantity ] )
					  ;    #  save this till end of delay loop
					print "$SermonTitle will be printed shortly .... /n";

					# printScripts( $PDFfile, $quantity );
				}    # End of ScriptFileTable query
			}
			else
			{

				# No entry in ScriptFileTable
				$errMsg = "Cannot find $scriptID for CatalogID $CATID \n";
				print $errMsg;    #### Debug
			}
			$QScriptFile->finish;
			$QScriptFile = undef;
			print "\n";
		}    # end of while loop in Script table
	}
	else
	{       # no entry in the CD Table
		$errMsg = "Cannot find CatalogID $CATID \n";
	}
	$QScript->finish;
	$QScript = undef;

	#		print "Before return: Error msg is = $errMsg (?)\n";
	return $errMsg;
}
######################################################
# Send eMail (Error Messages)
######################################################
sub sendMail
{
	my $recipient = $_[0];
	my $subject   = $_[1];
	my $mailData  = $_[2];
	my $SendCC    = $_[3];


	return;


	if ( $SendCC == 0 )
	{
		return;    # do not do auto reply
	}

	# 		print "Send eMail ............\n";
	#		print "Recipient: $recipient \n";
	#		print "Subject:  $subject \n";
	#		print "$mailData\n";
	my $smtp = Net::SMTP->new($outServer);    # connect to an SMTP server

	#	$smtp->auth($account ,$mailpassword );
	$smtp->mail($fromRimage);                 # use the sender's address here

	#
	#		Incase of error, to Rob and me
	if ( $SendCC > 0 )
	{
		print "Sending error msg to @ErrorCC\n";
		$smtp->recipient(@ErrorCC);            # recipient's address
	}
	else
	{
		print "Sending status msg to $recipient\n";
		$smtp->recipient($recipient);          # No error, must be status, etc
	}
	$smtp->data();                            # Start the mail

	# Send the header.
	$smtp->datasend( "To: " . $recipient . "\n" );
	if ( $SendCC > 0 )
	{
		$smtp->datasend( "cc: " . "; " . $David . "\n" );
	}
	else
	{
		$smtp->datasend( "cc: " . "\n" );
	}
	$smtp->datasend( "From: " . $fromRimage . "\n" );
	$smtp->datasend( "Subject: " . $subject . "\n" );
	$smtp->datasend("\n");    # end of header

	# Send the body.
	$smtp->datasend($mailData);
	$smtp->dataend();         # Finish sending the mail
	$smtp->quit;
}
######################################################
# End Send Mail
######################################################
##########################################
# Analyze email header
##########################################
sub analyze_header
{
	my $header_array_ref = shift;
	my $header = join "", @$header_array_ref;
	my ($subject) = $header =~ /Subject: (.*)/m;
	my ($from)    = $header =~ /From: (.*)/m;
	my ($status)  = $header =~ /Status: (.*)/m;
	if ( defined $status )
	{
		$status = "Unread"                  if $status eq 'O';
		$status = "Read"                    if $status eq 'R';
		$status = "Read"                    if $status eq 'RO';
		$status = "Ne    $status = " - ";w" if $status eq 'NEW';
		$status = "New"                     if $status eq 'U';
	}
	else
	{
		$status = "-";
	}
	return ( $subject, $from, $status );
}
######################################################
# Trim blank spaces
######################################################
# Perl trim function to remove whitespace from the start and end of the string
sub trim($)
{
	my $string = shift;
	$string =~ s/^\s+//;
	$string =~ s/\s+$//;
	return $string;
}

# Left trim function to remove leading whitespace
sub ltrim($)
{
	my $string = shift;
	$string =~ s/^\s+//;
	return $string;
}

# Right trim function to remove trailing whitespace
sub rtrim($)
{
	my $string = shift;
	$string =~ s/\s+$//;
	return $string;
}
#############################################################
# Subroutine to print a PDF file
#
# This routine is used to process/print a pdf file
# It calls Adobe Reader to format and print
# However, Adobe Reader does not terminate itself after print
# we have to kick off the thread for Adobe
#
#############################################################
#############################################################
#
# Adobe Acrobat SDK
# How do I use the Windows command line with Acrobat and Adobe'Reader?
# You can display and print a PDF file with Acrobat and Adobe Reader
# from the command line. These commands are unsupported, but have worked
# for some developers. There is no documentation for these commands other
# than what is listed below.
#
# Note:All examples below use Adobe Reader, but apply to Acrobat as well.
# If you are using Acrobat, substitute Acrobat.exe in place of AcroRd32.exe on the command line.
# AcroRd32.exe pathname --  Start Adobe Reader and display the file. The full path must be provided.
# This command can accept the following options.
#  Option  					Meaning
#   /n    Start a separate instance of Acrobat or Adobe Reader, even if one is currently open.
#   /s    Suppress the splash screen.
#   /o    Suppress the open file dialog box.
#   /h    Start Acrobat or Adobe Reader in a minimized window.
#
#  AcroRd32.exe /p pathname -- Start Adobe Reader and display the Print dialog box.
#  AcroRd32.exe /t path "printername" "drivername" "portname" -- Start Adobe Reader and
#      print a file while suppressing the Print dialog box. The path must be fully specified.
#
#  The four parameters of the /t option evaluate to path, printername, drivername, and
#  portname (all strings).
#
#	printername -- The name of your printer.
#	drivername --  Your printer driver's name, as it appears in your printer's properties.
#	portname --  The printer's port. portname cannot contain any "/" characters; if it does, output is routed to the default port for that printer.
#
#
#############################################################
my $current  = 0;
my $pid      = 0;
my $childpid = 0;
sub StartSubTask($);
sub printPDF;
$SIG{CHLD} = \&REAPER;

sub printScripts
{

	#   print "\n\nStill cannot process transript !!!!!\n\n";
	#   return;
	# print Script
	# Get calling parameters
	# We cannot call print directly from here
	# We call the subroutine printPDF which inturn kick off
	# a thread for Adobe Reader
	#
	my ( $Copies, $PDFfile );
	my $systemcall;
	$PDFfile = $_[0];    # print this file
	$Copies  = $_[1];    # Number of copies for this order
	#############################################################
	# Update print time
	#############################################################
	# Now just in case we have to a dummy print
	$currenttime = time;
	$elapsedTime = $currenttime - $LastPrintTime;
	if ( $elapsedTime > $printDelayTime )
	{
		$systemcall = $GScall . "\"" . $dummyFile . "\"";

		# Print a dummy file to warm up the printer
		#	printPDF($dummyFile);    # Print this file
		print "Printing $dummyFile\n ";

# system( $AdobeReader . " /p /h /o /s " . $dummyFile ) ;    # Print with AdobeReader
		system($systemcall);    # Print with ghost script
		print "Return from System call \n";
	}
	$systemcall = $GScall . "\"" . $PDFfile . "\"";
	while ($Copies)
	{

		#		printPDF($PDFfile);      # Print this file
		print "Printing $PDFfile\n ";

# system( $AdobeReader . " /p /h /o /s " . $PDFfile ) ;    # Print with AdobeReader
		system($systemcall);    # Print with ghost script
		print "Return from System call \n";
		$Copies--;
	}
	$LastPrintTime = time;
}
my (
	  $BatchNbr, $BatchStatus, $ApplyDate, $OrdNbr,
	  $PurQty,   $FreeQty,     $ProductID, $ProdCode
);
my ( $TotalQty, $type, @Line );
#############################################################
#
# Process MPX Batch Records from SQL Server
#
#############################################################
sub ProcessMPX
{
	my $SQLError;
	my $BatchRef;
	my ($rc, $eMsg);
	# 	Look back $DaysAgo days for batch records
	my $Days;
	my $DaysAgo = 7;

	# Since there are 86,400 seconds in a day
	# No. of days ago will be
	my (
		  $second,     $minute,    $hour,
		  $dayOfMonth, $month,     $yearOffset,
		  $dayOfWeek,  $dayOfYear, $daylightSavings
	  )
	  = localtime( time - ( 86400 * $DaysAgo ) );
	$year = 1900 + $yearOffset;
	$month++;    # 0 index
	$Days = "'$month/$dayOfMonth/$year'";

	# Prepare Batches
	my $prepareBatches =
"SELECT BatchNbr, BatchStatus, ApplyDate FROM $Batches WHERE BatchStatus = 'posted' and ApplyDate > $Days ";
	print "\n$currenttime - Starting SQL Server \n";

	# print "Call connectSQLTable\n";
	if ( !defined $MSdbh )    # if not already connected, call connectTable
	{
		$SQLError = connectSQLTable();    # call to connect mySQL
		if ( $SQLError != 0 )
		{

			# close SQL Server connection
			# Error message printed by connectSQL
#			$MPXErrStr = "Cannot connect SQL Server ables rc = $SQLError \n";
#			print "$MPXErrStr";
			$noMPX++;
			return -1;
		}
	}

	# print "Call connect to MySQL Tables\n";
	if ( !defined $dbh )    # if not already connected, call connectTable
	{
		$SQLError = connectTable();    # call to connect mySQL
		if ( $SQLError ne "" )
		{

			# close SQL Server connection
			# Error message printed by connectSQL
#			$MPXErrStr = "Cannot connect MySQL Server ables rc = $SQLError \n";
#			print "$MPXErrStr";
			return -1;
		}
	}

	# search for Batches
	# See if we see this batch record been processed
	print "Looking for MPX Batches posted since $Days ($DaysAgo days ago)\n"
	  ;    #### Debug

	#
	# Check with Batches Table, make sure this item is defined
	#
	#
	# Check with Batches Table, make sure this item is defined
	#
	# print "Query Batches\n";
	if ( !defined $QBatches )    # if first time since connection
	{
		$QBatches = $MSdbh->prepare($prepareBatches) ;
# DCW		  or die "Can't prepare SELECT $Batches: $MSdbh->errstrn";
		if (!defined $QBatches)
		{
			$MPXErrStr = "MS SQL Can't prepare SELECT $Batches: $MSdbh->errstrn";
			print "$MPXErrStr";
			$noMPX++;	#Cannot connect
			return -1;
		}
	}
	$QBatches->execute()
	  or die "can't execute the query: $QBatches->errstr";
	if ( $QBatches->rows == 0 )
	{
		print "Batches Record table is empty\n";
		return 0;
	}
	else
	{
		$BatchRef = $QBatches->fetchall_arrayref();
		$QBatches->finish;
		$QBatches = undef;
	}
	$errMsg = "";    # found at least one batches record

	#	print "Find at least 1 batchNbr \n";    #### Debug
	foreach my $BatchRow (@$BatchRef)
	{

		#
		# Find at least 1 batchNbr ";
		#
		( $BatchNbr, $BatchStatus, $ApplyDate ) = @$BatchRow;

		#			print "Find batchNbr $BatchNbr, $BatchStatus, $ApplyDate\n";
		#
		# Now see if this record is in myBatches
		#
		# print "\nNow see if this record is in myBatch\n";
		if ( !defined $QMyBatches )    # if first time since connection
		{

			#			print "Line: ", __LINE__, "\n";
			$QMyBatches = $dbh->prepare($prepareMyBatches);
#	DCW		  or die "Can't prepare SELECT $MyBatches: $dbh->errstrn";
			if (!defined $QMyBatches )
			{
				$MPXErrStr = "QMyBatches: Can't prepare SELECT $MyBatches: $dbh->errstrn";
				print "$MPXErrStr";
				$noMPX++;	#Cannot connect
				return -1;
			}
		}

		#		print "Line: ", __LINE__, "\n";
		$QMyBatches->execute($BatchNbr)
		  or die "can't execute the query: $QMyBatches->errstr";

		#		print "Line: ", __LINE__, "\n";
		if ( $QMyBatches->rows == 0 )
		{
			$NoBatchRc++;    # Update status
			my $MyApplyDate = MyDateTime();    # Get Time Stamp

			#			print CDLOGFILE
			#			  "\n\nFind batchNbr $BatchNbr, $BatchStatus, $ApplyDate\n";
			print "$MyApplyDate - Batch Number: $BatchNbr\n";

			# This record is not in MyBatch so we have to process it
			$errMsg      = "";
			$currenttime = MyDateTime();
			print CDLOGFILE
			  "\n$currenttime Start processing BatchRecord $BatchNbr\n";
			$rc = GetOrderHeader($BatchNbr);
			if ( $rc ne "" )
			{
				$errMsg = $errMsg . $rc;
				$NoError++;
				$eMsg = "Cannot process BatchRecord #$BatchNbr:\n$errMsg";
				print "$eMsg\n";
				print CDLOGFILE "$eMsg\n";
				print ERRLOGFILE "$MyApplyDate : $eMsg\n";

				######################################################################
				#  send an error email as well
				######################################################################

				$emailSubj = $status . " : ERROR\n";
				$emailBody = "$MyApplyDate : $eMsg\n";

				# send an email out
				sendMail( "rimage2", $emailSubj, $emailBody, 1 );

			}

			# We are going to insert this record just the same
			#
			print "Now we have to update myBatches\n";

			#
			$QInsertBatches = $dbh->prepare($prepareInsertMyBatch)
			  or die "Can't prepare $prepareInsertMyBatch  $dbh->errstrn";
			$QInsertBatches->execute( $BatchNbr, $BatchStatus, $MyApplyDate )
			  or die "can't execute the query: $QMyBatches->errstr";
			print "Successful insert BatchRecord $BatchNbr\n";
			print CDLOGFILE "Successful insert BatchRecord $BatchNbr\n";
			$QInsertBatches->finish;
			$QInsertBatches = undef;
		}
		else
		{

			#			print "BatchNbr $BatchNbr is in myBatch record, skip this file\n";
		}
	}    # end of Batch Records in MPX
	$QMyBatches->finish;

	# Now release combined orders
	$errMsg = ReleaseOrders();

	disconnectTable();
	disconnectSQLTable();
	print "\nEnd of ProcessMPX:\n";
	if ( $errMsg ne "" )
	{
		print "Error msg is = $errMsg\n";
	}
	return $errMsg;
}

sub GetOrderHeader
{

	# Prepare OrderHeader Table
	my $prepareOrderHeader =
	  "SELECT OrderId, BatchId FROM $OrderHeader WHERE BatchId = ?";

	#
	# Now Look into OrderHeader
	#
	# Get this batchNbr
	my $thisBatchId = $_[0];
	my $foundBatchId;
	my $rc = "";
	if ( !defined $QOrderHeader )    # if first time since connection
	{
		print "Prepare QOrderHeader \n";
		$QOrderHeader = $MSdbh->prepare($prepareOrderHeader);
# DCW		  or die "Can't prepare SELECT $OrderHeader: $MSdbh->errstrn";
		if (!defined $QOrderHeader)
		{
			$MPXErrStr = "GetOrderHeader: Can't prepare SELECT $OrderHeader: $MSdbh->errstrn";
			$noMPX++;
			return "Can't prepare SELECT $OrderHeader\n";
		}
	}
	print "Execute OrderHeader\n";
	$QOrderHeader->execute($thisBatchId)
	  or die "can't execute the query: $QOrderHeader->errstr";
	if ( $QOrderHeader->rows != 0 )
	{
		print "Found at least 1 batchID to be processed\n";
		my $OrderHeaderRef = $QOrderHeader->fetchall_arrayref();
		$QOrderHeader->finish;
		$QOrderHeader = undef;
		foreach my $HeaderRow (@$OrderHeaderRef)
		{
			$NoRecords++;
			( $OrderId, $foundBatchId ) = @$HeaderRow;

		  #			print CDLOGFILE
		  #			  "Found Ordnbr $OrderId $foundBatchId in BatchId $thisBatchId\n";
			print
			  "Found Ordnbr $OrderId $foundBatchId in BatchId $thisBatchId\n";

			#
			# now look for this OrderId in OrderDetail
			#
			print "Now look for this OrderId in OrderDetail\n";
			$rc = "";
			$rc = GetOrderDetail($OrderId);
			if ( $rc ne "" )
			{
				print "$rc\n";
				$errMsg = $errMsg . "\t$rc";

				# keep looping
			}
		}
	}
	else
	{
		$NoError++;
		$rc =
"GetOrderHeader: Cannot find any OrderHeader for BatchId $thisBatchId\n";
		$errMsg = $errMsg . "\t$rc";
		print " $errMsg \n";
		print CDLOGFILE " $errMsg \n";
		$QOrderHeader->finish;
		$QOrderHeader = undef;
	}
	print "\nEnd of GetOrderHeader\n";
	return $errMsg;
}

sub GetOrderDetail
{
	my $thisOrderId;
	my $rc = "";

	# Prepare OrderDetail
	my $prepareOrderDetail =
		"SELECT OrderId, Quantity, ProductID FROM $OrderDetail WHERE OrderId = ?";
	$thisOrderId = $_[0];    # Get orderNbr wanted
	my $foundNbr;
	if ( !defined $QOrderDetail )    # if first time since connection
	{
		$QOrderDetail = $MSdbh->prepare($prepareOrderDetail);
#	DCW	  or die "Can't prepare SELECT $OrderDetail: $MSdbh->errstrn";
		if (!defined $QOrderDetail )
		{
			$MPXErrStr = "GetOrderDetail: Can't prepare SELECT $OrderDetail: $MSdbh->errstrn";
			print "$MPXErrStr";
			$noMPX++;
			return "GetOrderDetail: Can't prepare SELECT $OrderDetail\n";
		}
	}
	$QOrderDetail->execute($thisOrderId)
	  or die "can't execute the query: $OrderDetail->errstr";
	if ( $QOrderDetail->rows != 0 )
	{
		my $DetailRef = $QOrderDetail->fetchall_arrayref();
		$QOrderDetail->finish;
		$QOrderDetail = undef;
		foreach my $DetailRow (@$DetailRef)
		{
			( $foundNbr, $Quantity, $ProductID ) = @$DetailRow;

			# print CDLOGFILE
			# 	"Orderdetail: $foundNbr, $Quantity, $ProductID\n";
			print "Orderdetail: $foundNbr, $Quantity, $ProductID\n";
			$TotalQty = $Quantity;

			#
			# Now look for this PorductID in the Products
			#
			print "Now look for this $ProductID in the Products, Qty $TotalQty\n";
			$rc = "";
			$rc = GetProducts( $ProductID, $TotalQty );
			if ( $rc ne "" )
			{
				$NoError++;
				print "$rc\n";
				print CDLOGFILE "$rc\n";
				$errMsg = $errMsg . "\t$rc";
			}
		}
	}
	else
	{
		$NoError++;
		$rc =
"GetOrderDetail: Cannot find any OrderDetail for OrderNbr $thisOrderId\n";
		print "$rc\n";
		$errMsg = $errMsg . $rc;
		print CDLOGFILE "$errMsg\n";
		$QOrderDetail->finish;
		$QOrderDetail = undef;
	}
	print "\nEnd of GetOrderDetail\n";
	return "$errMsg";
}

sub GetProducts
{
	my $thisProductID = $_[0];
	my $thisQuantity  = $_[1];
	my $foundID;
	my $rc = "";

	# Prepare Products
	my $prepareProducts =
	  "SELECT ProductID, ProdCode FROM $Products WHERE ProductID = ?";
	$errMsg = "";
	if ( !defined $QProducts )    # if first time since connection
	{
		$QProducts = $MSdbh->prepare($prepareProducts);
# DCW		  or die "Can't prepare SELECT $Products: $MSdbh->errstrn";
		if ( !defined $QProducts )
		{
			$MPXErrStr = "GetProducts: Can't prepare SELECT $Products: $MSdbh->errstrn";
			print "$MPXErrStr";
			$noMPX++;
			return "GetProducts: Can't prepare SELECT\n";
		}
	}
	$QProducts->execute($thisProductID)
	  or die "can't execute the query: $QProducts->errstr";
	if ( $QProducts->rows != 0 )
	{
		my $ProductsRef = $QProducts->fetchall_arrayref();
		$QProducts->finish;
		$QProducts = undef;
		foreach my $ProductRow (@$ProductsRef)
		{
			$rc = "";
			( $foundID, $ProdCode ) = @$ProductRow;

			#			print CDLOGFILE
			#			  "Found the ProducID $foundID with productCode $ProdCode\n";
			print "Found the ProducID $foundID with productCode $ProdCode\n";
			@Line = split( /\s+/, $ProdCode );

			#
			# Now get the type field from the product code
			#
			$ProdCode = shift(@Line);    # Get the first part
			$type     = shift(@Line);    # get the seco
			print "Rimage Prodcode $ProdCode, Type $type\n";

	 #			print CDLOGFILE
	 # 				"\tOrderNbr: $OrderId Product Code : $ProdCode $type $thisQuantity \n";
	 #
	 # Now we can process this Product Code in MySQL
	 #
	 # call process order with $Prodcode $type $thisQuantity
	 #
			print CDLOGFILE "Processorder with $ProdCode $type $thisQuantity\n";
			if (( $type =~ /C/i ) || ( $type =~ /D/i )|| ( $type =~ /V/i )) # CD, mp3(data) or Video
			{
#				$rc = ProcessOrder( $ProdCode, $thisQuantity );
				# Combine same product orders
				# This queue will be released at the end of ProcessMPX
				$rc = CombineOrders( $ProdCode, $thisQuantity );
			}
			else
			{
				if ( $type =~ /T/i )
				{
					$rc = ProcessTranscript( $ProdCode, $thisQuantity );
				}
				else
				{
					# if  Book or MP3 dowload, discard order
					if (!( ($type =~ /B/i )||( $type =~ /M/i )))
					{
						print "Cannot process this Product:$ProdCode, Type:$type\n";
						print CDLOGFILE
					  "Cannot process this Product:$ProdCode, Type:$type\n";
					}
				}
			}
			if ( $rc ne "" )
			{
				print "Error return from ProcessOrder\n %rc\n";
				$errMsg = $errMsg . $rc;
			}
		}    # End of Products table while
	}
	else
	{
		$NoError++;
		$rc     = "GetProducts: Cannot find $ProductID in Produsts table\n";
		$errMsg = $errMsg . $rc;
		print "$errMsg \n";
		print CDLOGFILE "$errMsg \n";
	}
	print "\nEnd of GetProducts\n";
	return $errMsg;
}    # end of while loop in OrderHeader table

sub CombineOrders
{
	###################################################################
	# Combine for same catalog orders
	###################################################################
	my $thisProductID = $_[0];
	my $thisQuantity  = $_[1];
	my $found         = 0;

 DUPLOOP: foreach $orderListElement (@orderList)
	{
		print "orderlist $orderListElement->[0] $orderListElement->[1]\n";
		if ( $thisProductID eq $orderListElement->[0])    # if this order ready in the queue
		{
			# This order is in the queue
			# just update the quanity
			print "Found order $orderListElement->[0] qty = $orderListElement->[1]\n";
			$orderListElement->[1] = $orderListElement->[1] + $thisQuantity;
			$found = 1;      # found the order
			last DUPLOOP;    # exit this loop
		}
	}
	if ( $found == 0 )
	{
		# did not find the order in the list, must be new
		print "Push new order $thisProductID\n";
		push( @orderList, [ $thisProductID, $thisQuantity ] );
	}
	return ("");
}

sub ReleaseOrders
{
	my $rc = "";
	#
	# Connect to MySQL
	#
	if ( !defined $dbh )    # if not already connected, call connectTable
	{
		$rc = connectTable();    # call to connect mySQL
		if ( $rc ne "" )
		{
			# close POP3 connection
			$pop3->quit();
			$rc = "Cannot connect wisdom SQL tables rc = $rc \n";
			print CDLOGFILE "$errMsg";

			#			print ERRLOGFILE "$errMsg";
			return $rc;
		}
	}
	foreach $orderListElement (@orderList)
	{
		my ($thisProductID, $thisQuantity ) = @$orderListElement ;
		print "ProcessOrder ($thisProductID, $thisQuantity)\n";	# Process the CD
		$rc = ProcessOrder ($thisProductID, $thisQuantity);	# Process the CD
		if ($rc ne "")
		{
			print CDLOGFILE "$rc\n";
			print ERRLOGFILE "$rc\n";
			$rc = "";
		}
	}
	@orderList = ();		# clear list
	return $rc ;
}
#####################################################
# 	Connect to MySQL tables
#####################################################
sub connectTable
{
	#############################################################
	## SQL query
	#############################################################
	$dbh =
	  DBI->connect( "DBI:mysql:$db:$host; Database=$myDataBase", $user, $pass )
	  	or die "Cannot connect to MySQL Server database $DBI::errstr";
	#
	# use my Data Base
	#
	$sqlQuery = $dbh->prepare("use $myDataBase")
	  or die "Can't prepare use $myDataBase: $dbh->errstrn";
	$sqlQuery->execute
	  or die "can't execute the query: $sqlQuery->errstr";

	# $rc = $sqlQuery->finish;
	$sqlQuery->finish;
	$sqlQuery = undef;
	print "Connected to MySQL\n";
	## OK using myDataBase
	return "";
}
######################################################
# Disconnect from data base
######################################################
sub disconnectTable
{
	if ( defined $dbh )
	{
		$dbh->disconnect;
		print "Disconnect MySQL connection\n";
	}

	# undefine SQL prepares
	$dbh          = undef;
	$QueryCD      = undef;
	$QAudio       = undef;
	$QAudioFile   = undef;
	$QCatalog     = undef;
	$QScript      = undef;
	$QScriptFile  = undef;
	$QMyBatches   = undef;
	$QComboSeries = undef;

	#   print "Disconnect MySQL handles\n";
}
#####################################################
# 	Connect to SQL Server tables
#####################################################
sub connectSQLTable
{

	# this is new
	#############################################################
	#	SQL Server DATA
	#############################################################

	#############################################################
	## SQL query
	#############################################################

	#  Connect to MPX data base
	$MSdbh = DBI->connect( $MS_DSN, $MSuser, $MSpass );
# DCW		  or die "Cannot connect at SQL Server database $DBI::errstr";

	#  Connect to test data base
#	$MSdbh = DBI->connect( "DBI:mysql:mysql:localhost;Database=mpx",
#								  "root", "WftH2001" );
## DCW	  or die "Cannot connect at SQL Server database $DBI::errstr";

	if (!defined $MSdbh )
	{
		$MPXErrStr = "Cannot connect at SQL Server database\n\t$DBI::errstr\n";
		print "$MPXErrStr";
		$noMPX++;		# no ManPower connected
		return -1;
	}
	# use my Data Base
	#
	$MSsqlQuery = $MSdbh->prepare("use $MSDataBase") ;
# DCW	  or die "Can't prepare use $MSDataBase: $MSdbh->errstrn";
	if (!defined  $MSsqlQuery )
	{
		print "Cannot prepare use $MSDataBase: $MSdbh->errstrn\n";
		$noMPX++;		# no ManPower connected
		return -1;
	}

	# $rv = $MSsqlQuery->execute
	#   or die "can't execute use : $MSsqlQuery->errstr";
	$MSsqlQuery->execute
	  or die "can't execute the query: $MSsqlQuery->errstr\n";

#	print "MS SQL execute error code = $MSdbh->err()\n";
#	if ($MSdbh->err() != 0)
#	{
#		print "Cannot execute the query: $MSsqlQuery->errstr()\n";
#		$noMPX++;		# no ManPower connected
#		return -1;
#	}

	$MSsqlQuery->finish;
	$MSsqlQuery = undef;
	print "Connected to SQL Server\n";
	## OK using MSDataBase
	return 0;
}
######################################################
# Disconnect from data base
######################################################
sub disconnectSQLTable
{
	if ( defined $MSdbh )
	{
		$MSdbh->disconnect;
		print "Disconnect SQL Server connection\n";
	}

	# undefine SQL prepares
	$MSdbh        = undef;
	$QBatches     = undef;
	$QOrderHeader = undef;
	$QOrderDetail = undef;
	$QProducts    = undef;
	print "Disconnect SQL Server handles\n";
}
######################## Not Used ##############################
sub printPDF
{

	# This routine is used to kick off thread
	my $printFile = $_[0];
	StartSubTask( $AdobeReader . " /p /h /o /s " . $printFile );
	print "Return from StartSubTask (Adobe Reader)\n ";    #debug
	sleep 30;
	kill $childpid;

	#	print "killed child pid = $childpid \n";
}

#
# sub: A child process has finished
#
sub REAPER
{
	wait;
	$current--;

	# loathe sysV: it makes us not only reinstate
	# the handler, but place it after the wait
	$SIG{CHLD} = \&REAPER;
}
######################################################################
#   S U B R O U T I N E S
######################################################################
#
# sub: Print status (Runnung+Waiting)
#
#
# sub: Start a new child process
#
sub StartSubTask($)
{
	my $prog = shift;
	print "starting this :\n\t$prog \n";
	$childpid = fork();
	if ( $childpid == 0 )
	{

		# This is a child thread
		$pid = $$;

		#		print "Hi, I'm the child! PID = $$\n";
		#		print "Child pid = $pid\n";
		close STDOUT;
		system "$prog";    # Execute system call
		exit 0;
	}
	else
	{

		# This is the PARENT thread
		$current++;

		#		print "Parent PID = $childpid, exec $prog\n";
	}
}
