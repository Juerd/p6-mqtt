use v6;
use Net::MQTT::MyPack;

unit class Net::MQTT;

has Int     $.keepalive-interval    is rw = 60;
has Int     $.maximum-length        is rw = 2097152;  # 2 MB
has Str     $.client-identifier     is rw = "perl6-$*PID";
has Str     $.server                is rw;
has Int     $.port                  is rw = 1883;
has Supply  $.messages;
has Supply  $!packets;
has Buf     $!buf;
has Promise $.connection;

submethod BUILD (:$!server, :$!port = 1883) {
    say $!server;
    say $!port; 

    $!messages .= new;
    $!packets .= new;
    $!buf .= new;

    $!packets.tap: {
        if (.<type> == 3) {  # published message
            my $topic-length = .<data>.unpack("n");
            my $topic   = .<data>.subbuf(2, $topic-length).decode("utf8-c8");
            my $message = .<data>.subbuf(2 + $topic-length);
            my $retain  = .<retain>;
            $!messages.emit({ :$topic, :$message, :$retain });
        }
    };

    $!connection = IO::Socket::Async.connect($!server, $!port).then( -> $p {
        return if not $p.status;

        my $socket := $p.result;

        react {
            my $bs := $socket.bytes-supply;

            whenever $bs -> $received {
                $!buf ~= $received;
                self._parse;
            }

            $socket.write: mypack "C m/(n/a* C C n n/a*)", 0x10,
                "MQIsdp", 3, 2, $!keepalive-interval, $!client-identifier;
            $socket.write: mypack "C m/(n/a* a*)", 0x30,
                "hello-world", "Hi there!";
            $socket.write: mypack "C m/(C C n/a* C)", 0x82,
                0, 0, "typing-speed-test.aoeu.eu", 0;
            $socket.write: mypack "C m/(C C n/a* C)", 0x82,
                0, 0, "revspace/#", 0;

            my $ping := Supply.interval($!keepalive-interval);
            $ping.tap: {
                say "Sending keepalive";
                $socket.write: pack "C x", 0xc0;
            };
        }
        $socket.close;
    });
}

method _parse {
    my $offset = 1;

    my $multiplier = 1;
    my $length = 0;
    my $d;
    {
        return if $offset >= $!buf.elems;
        $d = $!buf[$offset++];
        $length += ($d +& 0x7f) * $multiplier;
        $multiplier *= 128;
        redo if $d +& 0x80;
    }
    return if $length > $!buf.elems + $offset;

    my $first_byte = $!buf[0];

    my $packet := hash {
        type   => ($first_byte +& 0xf0) +> 4,
        dup    => ($first_byte +& 0x08) +> 3,
        qos    => ($first_byte +& 0x06) +> 1,
        retain => ($first_byte +& 0x01),
        data   => $!buf.subbuf($offset, $length);
    };

    $!buf .= subbuf($offset + $length);

    $!packets.emit($packet);
}
