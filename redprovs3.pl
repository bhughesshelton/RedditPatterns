use strict;
use warnings;
use feature 'say';
use diagnostics;
my @packages= ("Parallel::ForkManager", 
			   "use Benchmark::Timer",
			   "DBI");
foreach my $package (@packages) {
	eval {"use $package; 1"
	} or do {
		say "I need to install CPAN modules. I can try to resolve this automatically. If this fails, you need to install $package from CPAN on your own";
		say "Do you want me to try to get $package from CPAN? Y/N:";
		my $yn = <STDIN>;
		chomp $yn;
		$yn = lc $yn;
		if ($yn == "y"){
			my @args = ("perl", "-MCPAN", "-e", "install $package");
			system @args;
		} else {die "Suit yourself. You need to install $package. Thus, I die."};
	}
} 
use Parallel::ForkManager;
use Benchmark::Timer;
use DBI;
use Fcntl qw(:flock SEEK_END);
my $wordCount;
my $matches;
my $lineNum;
my $nth = 1;
my $start = time;														#Set start time for Benchmarking
my $dir = "./";
my @fps = (glob("$dir*.json"));  										#Glob in array of Files
say "Please enter your MySQL user name:";
my $username = <STDIN>;
say "Please enter your MySQL password:";
my $password = <STDIN>;
say "Please enter the name of the db you want to create:";
my $db = <STDIN>;
say "How many cores do you want to use?";
my $Nforks = <STDIN>; 
chomp ($username, $password, $db, $Nforks);
my $fm = Parallel::ForkManager->new($Nforks);
my $dsn = "DBI:mysql:Driver={SQL Server}";
my %attr = (PrintError=>0, RaiseError=>1);
my $dbh = DBI->connect($dsn,$username,$password, \%attr);
$dbh->{mysql_enable_utf8} = 1;
$dbh->{InactiveDestroy} = 1;

my @ddl = (
	"CREATE DATABASE IF NOT EXISTS $db;",
	
	"USE $db;",

	"CREATE TABLE IF NOT EXISTS proverbs 
	 (prov varchar(255),
	 date varchar(255),
	 time varchar(255),
	 sub varchar(255), 
	 author varchar(255),
	 hit varchar(1000),
	 rawmatch varchar(1000)) ENGINE=InnoDB;"
);

for my $sql(@ddl){
  $dbh->do($sql);
}
say "All tables created successfully!";
say "Sorting...Hang on, this could take a while....";

my @patterns = (
	 'to (.*?),?or not to (\\2).*',
	 '(an?|the) \\w+ ?(\\w+)? by any other name would.*',
	 'shall i compare thee to .*',
	 '((\\w+,? ){0,4})?(lend me your ears).*',
	 '((\\w+,? ){0,4})?(wherefore art thou).*',
	 'one \\w+ ?(\\w+)? swoop',
	 'all(\'| i)s ?(\\w+)? ?(\\w+)? ?(\\w+)? (that ends) ?(\\w+)? ?(\\w+)? ?(\\w+)?',
	 'n?either (an?|the) \\w+er n?or (an?|the) \\w+er be',
	 'shakespeare ?(has|once)? (said|says|writes|wrote|quot|saying|line).*'
	);
my @keys = qw(2B othername compthee lendme wherefore swoop endswell lender shax);

foreach (my $i = 0; $i < @fps; $i++) {  								#Set iterator for forks
	
	$fm->start and next;												#Start forking
	ParseJSON ($fps[$i]);												#Call to subs on array of files
	say "finished with $fps[$i]";
	my $duration = time - $start;
	if ($duration > 3600){
		$duration = $duration / 3600;        		 					#Benchmarking and Runtime
		say "\tI've been running for $duration hours now";
	}elsif($duration > 60){	
		$duration = $duration / 60;        
		say "\tI've been running for $duration minutes now";
	}else{
		say "\tI've been running for $duration seconds now";
	}
	$fm->finish;
}

$fm-> wait_all_children;
say "finished sorting";

$dbh->disconnect();
say "Finished with everything !!!!!!!!!!!!!!";

sub ParseJSON {
  	my $fp = shift;
	open my $fh, "<", $fp or die "can't read open '$fp': $_";										
	say "opening $fp";
	$wordCount = 0;
	$lineNum = 0;
	$matches = 0;
    my $sql = "INSERT INTO proverbs(prov, date, time, sub, author, hit, rawmatch) VALUES(?,?,?,?,?,?,?)";
	my $stmt = $dbh->prepare($sql);
    while (<$fh>) {      	
      	my $body;
      	$lineNum++;   													#Count how many lines there are.   	
		next unless $lineNum % $nth == 0;  								#if match process body
		if (m/\{.*"body":"(.*?)(?<!\\)"(,.*)?\}/){
		$body = $1;}
		next unless $body;
		$body = &specialCharacters($body);
		next if $body =~ /\A\s*\Z/;		
		foreach (my $i = 0; $i < @patterns; $i++){
			if ($body =~ m/($patterns[$i])/i){									#Get matches
				$matches++;
				my $rawmatch = $1;					
				my $match = lc $rawmatch;									#Clean up $rawmatch
				$match =~ s/ {2,}/ /g;
				$match =~ s/[[:punct:]]//g; 
				m/"subreddit":"(.+?)"/;
				my $sub = $1;
				m/"author":"(.+?)"/;
				my $author = $1;
				m/\{.*"created_utc":"?(\d+)"?,?.*\}/;
				my $dt = $1;
				my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($dt);
				$mon++; 
				$sec  = "0$sec"  if length($sec)==1;
				$min  = "0$min"  if length($min)==1;
				$hour = "0$hour" if length($hour)==1;
				$mday = "0$mday" if length($mday)==1;
				$mon  = "0$mon"  if length($mon)==1;
				my $date = $mon."/".$mday."/".($year+1900); 
				my $time = $hour.":".$min.":".$sec;
				my $stamp = $date." ".$time;
				$match = substr($match,0,125);
				$rawmatch = substr($rawmatch,0,125);
				my $prov = $keys[$i];
				#if(
					$stmt->execute($prov, $date, $time, $sub, $author, $match, $rawmatch);
					#){
				#	say "Something good happened with $prov : $match";
				#}	else {
				#	say "something fuckedup happpened with $prov";
				#}
			}	
		}
	}
}

sub specialCharacters {
	my $body = shift;
	
	# get rid of links
	$body =~ s/\[(.*?)\]\( ?https?:.*?\)/$1/g; 		# [text](http://link.com)
	$body =~ s/\( ?https?:.*?\)//g; 				# (http://link.com)
	$body =~ s/http\S*?\Z/ /g; 						# end of line
	$body =~ s/http\S*?\s/ /g; 						# any other free-standing ones
	
	# All html
	$body =~ s{\&lt;}{<}g;
	$body =~ s{\&gt;}{>}g;
	$body =~ s{\&amp;}{\&}g;
	$body =~ s{\&nbsp;}{ }g;
	
	# For all other cases, just turn them into white spaces.
	$body =~ s{\&[0-9a-z]+?;}{ };
	
	# I can't get unicode to work. I'll have to remove them for now.
	$body =~ s{\\u[0-9a-g]+}{}g;	
	
	# All escape characters
	$body =~ s{\\\"}{"}g;
	$body =~ s{\\n}{ }g;
	$body =~ s{\\r}{ }g;
	$body =~ s{\\t}{ }g;
	
	return $body;
}
