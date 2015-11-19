use v6;
use Net::MQTT::MyPack;

unit class Net::MQTT;

has Int     $.keepalive-interval    is rw = 60;
has Int     $.maximum-length        is rw = 2097152;  # 2 MB
has Str     $.client-identifier     is rw = "perl6-$*PID";
has Str     $.server                is rw;
has Int     $.port                  is rw;
has Supply  $.messages;
has Supply  $!packets;
has Buf     $!buf;
has Promise $.connection;
has Promise $!connected;
has Promise $!initialized;
has IO::Socket::Async $!socket;

submethod BUILD (Str:D :$!server, Int:D :$!port = 1883) {
    $!messages .= new;
    $!packets .= new;
    $!buf .= new;
    $!initialized .= new;

    $!packets.tap: {
        if (.<type> == 3) {  # published message
            my $topic-length = .<data>.unpack("n");
            my $topic   = .<data>.subbuf(2, $topic-length).decode("utf8-c8");
            my $message = .<data>.subbuf(2 + $topic-length);
            my $retain  = .<retain>;
            $!messages.emit({ :$topic, :$message, :$retain });
        }
    };
}

method connect () {
    $!connection = IO::Socket::Async.connect($!server, $!port).then: -> $p {
        if (not $p.status) {
            $!initialized.break;
            return;
        }

        $!socket := $p.result;
        self.initialize();

        react {
            my $bs := $!socket.bytes-supply;

            whenever $bs -> $received {
                $!buf ~= $received;
                self._parse;
            }
        }
        $!socket.close;
    };

    return $!initialized;
}

method initialize () {
    $!socket.write: mypack "C m/(n/a* C C n n/a*)", 0x10,
        "MQIsdp", 3, 2, $!keepalive-interval, $!client-identifier;
    $!socket.write: mypack "C m/(C C n/a* C)", 0x82,
        0, 0, "typing-speed-test.aoeu.eu", 0;
    $!socket.write: mypack "C m/(C C n/a* C)", 0x82,
        0, 0, "revspace/#", 0;

    my $ping := Supply.interval($!keepalive-interval);
    $ping.tap: {
        $!socket.write: pack "C x", 0xc0;
    };

    $!initialized.keep;
}

method _parse () {
    my $offset = 1;

    my $multiplier = 1;
    my $length = 0;
    my $d = 0;
    while ($d == 0 or $d +& 0x80) {
        return if $offset >= $!buf.elems;
        $d = $!buf[$offset++];
        $length += ($d +& 0x7f) * $multiplier;
        $multiplier *= 128;
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

multi method publish (Str $topic, Buf $message) {
    $!socket.write: mypack "C m/(n/a* a*)", 0x30, $topic, $message;
}

multi method publish (Str $topic, Str $message) {
    .publish($topic, $message.encode);
}

multi method retain (Str $topic, Buf $message) {
    $!socket.write: mypack "C m/(n/a* a*)", 0x31, $topic, $message;
}

multi method retain (Str $topic, Str $message) {
    .publish($topic, $message.encode);
}

