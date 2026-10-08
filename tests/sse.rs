use tansr_sdk::sse::Parser;

#[test]
fn every_byte_boundary_utf8_bom_crlf_and_multiline() {
    let input = "\u{feff}: keepalive\r\nid: 7\r\nevent: session.event\r\nretry: 1200\r\ndata: 汉😀\r\ndata: second\r\n\r\n".as_bytes();
    for split in 0..=input.len() {
        let mut parser = Parser::new(1024);
        let mut frames = parser.feed(&input[..split]).unwrap();
        frames.extend(parser.feed(&input[split..]).unwrap());
        frames.extend(parser.finish().unwrap());
        assert_eq!(frames.len(), 1, "split {split}");
        assert_eq!(frames[0].event.as_deref(), Some("session.event"));
        assert_eq!(frames[0].id.as_deref(), Some("7"));
        assert_eq!(frames[0].retry, Some(1200));
        assert_eq!(frames[0].data, "汉😀\nsecond");
        assert_eq!(parser.last_event_id(), "7");
    }
    let mut parser = Parser::new(1024);
    let mut frames = Vec::new();
    for byte in input {
        frames.extend(parser.feed(&[*byte]).unwrap());
    }
    assert_eq!(frames.len(), 1);
    assert!(parser.finish().unwrap().is_empty());
}

#[test]
fn cr_dispatches_on_open_connection_without_read_ahead() {
    let mut parser = Parser::new(64);
    let frame = parser.feed(b"data: first\r\r").unwrap();
    assert_eq!(frame.len(), 1);
    assert_eq!(frame[0].data, "first");
    assert!(parser.feed(b"\n").unwrap().is_empty());
    let frame = parser.feed(b"data: second\r\ndata: third\r\n\r").unwrap();
    assert_eq!(frame[0].data, "second\nthird");
    assert!(parser.feed(b"\n").unwrap().is_empty());
}

#[test]
fn empty_data_dispatches_but_id_only_does_not_and_empty_id_resets() {
    let mut parser = Parser::new(128);
    assert!(
        parser
            .feed(b"id: 42\nevent: unseen\nretry: 10\n\n")
            .unwrap()
            .is_empty()
    );
    assert_eq!(parser.last_event_id(), "42");
    let frames = parser.feed(b"data:\n\nid:\n\ndata: next\n\n").unwrap();
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].data, "");
    assert_eq!(frames[0].id, None);
    assert_eq!(frames[0].event, None);
    assert_eq!(frames[0].retry, None);
    assert_eq!(frames[1].data, "next");
    assert_eq!(parser.last_event_id(), "");
}

#[test]
fn nul_ids_ignored_retry_decimal_only_and_one_leading_space_removed() {
    let mut parser = Parser::new(1024);
    let frames = parser.feed(b"id: valid\nid: bad\0id\nretry: 0010\nretry: +1\nretry: 1.0\nretry: 18446744073709551616\nunknown: ignored\ndata:  one space\ndata\n\n").unwrap();
    assert_eq!(frames[0].id.as_deref(), Some("valid"));
    assert_eq!(frames[0].retry, Some(10));
    assert_eq!(frames[0].data, " one space\n");
    assert_eq!(parser.last_event_id(), "valid");
}

#[test]
fn bounds_include_comments_unknown_fields_and_pending_lines() {
    let mut parser = Parser::new(8);
    assert_eq!(parser.feed(b"data:a\n\n").unwrap().len(), 1);
    assert_eq!(parser.feed(b"data:b\n\n").unwrap().len(), 1);
    assert!(Parser::new(7).feed(b"data:a\n\n").is_err());
    assert!(Parser::new(5).feed(b":aaaaa").is_err());
    assert!(Parser::new(7).feed(b"x:aaaa\n\n").is_err());
    let mut parser = Parser::new(8);
    assert!(parser.feed(b"data:abc").unwrap().is_empty());
    assert!(parser.feed(b"d").is_err());
    assert!(parser.feed(b"\n\n").is_err());
}

#[test]
fn invalid_utf8_and_truncation_never_dispatch_partial_frame() {
    for input in [
        b"data: \xff\n\n".as_slice(),
        b":\xff\n",
        b"event: \xc0\xaf\n\n",
        b"id: \xed\xa0\x80\n\n",
    ] {
        assert!(Parser::new(100).feed(input).is_err());
    }
    for input in [
        b"data: partial".as_slice(),
        b"data: partial\n",
        b"data:\n",
        b"data: \xe6",
        b"\xef\xbb",
    ] {
        let mut parser = Parser::new(100);
        assert!(parser.feed(input).unwrap().is_empty());
        assert!(parser.finish().is_err());
        assert!(parser.feed(b"\n\n").is_err());
    }
    assert!(Parser::new(100).finish().unwrap().is_empty());
    let mut parser = Parser::new(100);
    assert_eq!(parser.feed(b"data: complete\n\n").unwrap().len(), 1);
    assert!(parser.finish().unwrap().is_empty());
    assert!(parser.finish().unwrap().is_empty());
    assert!(parser.feed(b"data: too late\n\n").is_err());
}

#[test]
fn bom_is_only_ignored_at_the_stream_start() {
    let mut parser = Parser::new(100);
    let frame = parser.feed("data: \u{feff}body\n\n".as_bytes()).unwrap();
    assert_eq!(frame[0].data, "\u{feff}body");
    let frame = parser
        .feed("\u{feff}data: not-a-data-field\n\n".as_bytes())
        .unwrap();
    assert!(frame.is_empty());
}
