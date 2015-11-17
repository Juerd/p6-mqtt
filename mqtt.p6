#!perl6

use v6;
use lib '.';
use MyPack;


await IO::Socket::Async.connect('test.mosquitto.org',1883).then( -> $p {
    return if not $p.status;
    given $p.result {
        react {
            my $bs = .bytes-supply;

            my Buf $buf .= new;
            my Supply $packets .= new;

            sub parse (Buf $buf is rw) {
                my $offset = 1;

                my $multiplier = 1;
                my $length = 0;
                my $d;
                {
                    return if $offset >= $buf.elems;
                    $d = $buf[$offset++];
                    dd $d;
                    $length += ($d +& 0x7f) * $multiplier;
                    dd $length;
                    $multiplier *= 128;
                    redo if $d +& 0x80;
                }
                return if $length > $buf.elems + $offset;

                my $first_byte = $buf[0];

                my $packet = hash {
                    type   => ($first_byte +& 0xf0) +> 4,
                    dup    => ($first_byte +& 0x08) +> 3,
                    qos    => ($first_byte +& 0x06) +> 1,
                    retain => ($first_byte +& 0x01),
                    data   => $buf.subbuf($offset, $length);
                };

                $buf .= subbuf($offset + $length);

                $packets.emit($packet);
            }

            .write(mypack("C m/(n/a* C C n n/a*)", 16, "MQIsdp", 3, 2, 60, "a"));

            my $hw = "hello-world " x 50;
            .write(mypack("C m/(n/a* a*)", 0x30, "hello-world", $hw));

            $packets.tap(-> $_ {
                say "Received packet of type {.<type>}";
            });

            whenever $bs -> $received {
                $buf ~= $received;
                $buf.say;
                parse $buf;
            }
        }
        .close;
    }
});
