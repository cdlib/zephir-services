import io
import sys

import pymarc
import pytest

from scripts.find_records_with_barcodes import find_records, open_input, parse_args

MARC_XML_TEMPLATE = """\
<?xml version="1.0" encoding="UTF-8"?>
<collection xmlns="http://www.loc.gov/MARC21/slim">
  <record>
    <leader>00000nam a2200000 a 4500</leader>
    <controlfield tag="001">{record_id}</controlfield>
    {barcode_field}
  </record>
</collection>
"""


def capture_stdout(monkeypatch):
    buffer = io.BytesIO()
    stdout = type("Stdout", (), {"buffer": buffer})()
    monkeypatch.setattr(sys, "stdout", stdout)
    return buffer


def write_marc_xml(path, record_id, barcode=None):
    barcode_field = ""
    if barcode is not None:
        barcode_field = (
            '<datafield tag="955" ind1=" " ind2=" ">'
            f'<subfield code="b">{barcode}</subfield>'
            "</datafield>"
        )
    path.write_text(
        MARC_XML_TEMPLATE.format(record_id=record_id, barcode_field=barcode_field)
    )


def output_record_ids(stdout_buffer):
    stdout_buffer.seek(0)
    records = pymarc.parse_xml_to_array(stdout_buffer)
    return [record["001"].data for record in records]


def test_parse_args_accepts_stdin_for_either_input():
    xml_list_from_stdin = parse_args(
        ["--xml-file-list", "-", "--barcodes-file", "barcodes.txt"]
    )
    barcodes_from_stdin = parse_args(
        ["--xml-file-list", "files.txt", "--barcodes-file", "-"]
    )

    assert xml_list_from_stdin.xml_file_list == "-"
    assert barcodes_from_stdin.barcodes_file == "-"


def test_parse_args_rejects_both_inputs_from_stdin(capsys):
    with pytest.raises(SystemExit) as exc_info:
        parse_args(["--xml-file-list", "-", "--barcodes-file", "-"])

    assert exc_info.value.code == 2
    assert "cannot both read from stdin" in capsys.readouterr().err


def test_open_input_does_not_close_stdin(monkeypatch):
    stdin = io.StringIO("input\n")
    monkeypatch.setattr(sys, "stdin", stdin)

    with open_input("-") as input_file:
        assert input_file.read() == "input\n"

    assert not stdin.closed


def test_find_records_reads_barcodes_from_stdin(tmp_path, monkeypatch):
    xml_file = tmp_path / "record.xml"
    xml_file_list = tmp_path / "xml-files.txt"
    write_marc_xml(xml_file, "record-from-barcodes-stdin", "requested-barcode")
    xml_file_list.write_text(f"{xml_file}\n")
    stdin = io.StringIO("requested-barcode\n")
    monkeypatch.setattr(sys, "stdin", stdin)
    stdout_buffer = capture_stdout(monkeypatch)

    find_records(xml_file_list, "-")

    assert output_record_ids(stdout_buffer) == ["record-from-barcodes-stdin"]
    assert not stdin.closed


def test_find_records_reads_xml_file_list_from_stdin(tmp_path, monkeypatch):
    xml_file = tmp_path / "record.xml"
    barcodes_file = tmp_path / "barcodes.txt"
    write_marc_xml(xml_file, "record-from-list-stdin", "requested-barcode")
    barcodes_file.write_text("requested-barcode\n")
    stdin = io.StringIO(f"{xml_file}\n")
    monkeypatch.setattr(sys, "stdin", stdin)
    stdout_buffer = capture_stdout(monkeypatch)

    find_records("-", barcodes_file)

    assert output_record_ids(stdout_buffer) == ["record-from-list-stdin"]
    assert not stdin.closed


def test_find_records_uses_later_record_and_reports_unreadable_barcode(
    tmp_path, caplog, monkeypatch
):
    first_xml = tmp_path / "first.xml"
    missing_barcode_xml = tmp_path / "missing-barcode.xml"
    last_xml = tmp_path / "last-fixed.xml"
    xml_file_list = tmp_path / "xml-files.txt"
    barcodes_file = tmp_path / "barcodes.txt"

    write_marc_xml(first_xml, "old-record", "requested-barcode")
    write_marc_xml(missing_barcode_xml, "missing-barcode")
    write_marc_xml(last_xml, "new-record", "requested-barcode")
    xml_file_list.write_text(
        f"{first_xml}\n{missing_barcode_xml}\n{last_xml}\n"
    )
    barcodes_file.write_text("requested-barcode\n")
    stdout_buffer = capture_stdout(monkeypatch)

    find_records(xml_file_list, barcodes_file)

    assert output_record_ids(stdout_buffer) == ["new-record"]
    assert str(missing_barcode_xml) in caplog.text
    assert "Could not read barcode for record 1" in caplog.text


def test_find_records_does_not_close_stdout(tmp_path, monkeypatch):
    xml_file = tmp_path / "record.xml"
    xml_file_list = tmp_path / "xml-files.txt"
    barcodes_file = tmp_path / "barcodes.txt"
    write_marc_xml(xml_file, "record", "requested-barcode")
    xml_file_list.write_text(f"{xml_file}\n")
    barcodes_file.write_text("requested-barcode\n")
    stdout_buffer = capture_stdout(monkeypatch)

    find_records(xml_file_list, barcodes_file)

    assert not stdout_buffer.closed
    stdout_buffer.write(b"still open")
