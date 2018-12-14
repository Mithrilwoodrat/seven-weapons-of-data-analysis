DOCS=$1
target=$2

function Main()
{
	echo "=============Start Test=============="

	if [ -a $DOCS ];then
	
		while read doc;
		do
            echo doc
			PUTDOC $doc
		done < $DOCS
		
	else
		echo "ERR no domain list"
	fi
}

function PUTDOC()
{
    doc=$1
    CMD="curl -uelastic:changeme -XPOST -H 'Content-Type: application/json'  -d \"$doc\" $target "
    echo "$CMD"
    $CMD
}

Main

end_time=`date -d "0 day" "+%Y-%m-%d %H:%M:%S"`
echo "The program start at $start_time,end at $end_time"
