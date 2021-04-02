import httmock
from gwvolman.lib.dataone.metadata import DataONEMetadata
from gwvolman.tests import mock_dataone_formats


def test_ctor():
    with httmock.HTTMock(mock_dataone_formats):
        node = "https://cn-stage-2.test.dataone.org/cn/"
        metadata = DataONEMetadata(node)
        assert len(metadata.mimetypes)
        assert metadata.resource_map is None
        assert metadata.coordinating_node == node


def test_strip_html_tags():
    html_str = "<head>getAhead</head>"
    expected = "getAhead"
    res = DataONEMetadata._strip_html_tags(html_str)
    assert res == expected


def test_get_directory():
    with httmock.HTTMock(mock_dataone_formats):
        node = "https://cn-stage-2.test.dataone.org/cn/"
        metadata = DataONEMetadata(node)
        assert not metadata.access_policy
        metadata.get_access_policy()
        assert metadata.access_policy


def test_get_access_policy():
    with httmock.HTTMock(mock_dataone_formats):
        node = "https://cn-stage-2.test.dataone.org/cn/"
        metadata = DataONEMetadata(node)
        assert not metadata.access_policy
        metadata.get_access_policy()
        assert metadata.access_policy


def test_generate_system_metadata():
    from d1_common.types import dataoneTypes

    with httmock.HTTMock(mock_dataone_formats):
        node = "https://cn-stage-2.test.dataone.org/cn/"
        metadata = DataONEMetadata(node)
        pid = "urn:uuid:3c5d3c8d-b6c2-4dff-ac28-9f2e60a157a1"
        name = "run-local.sh"
        format_id = "application/octet-stream"
        size = 1338
        md5 = "50321b197d014a1f3d7a3adf99277919,"
        rights_holder = "http://orcid.org/0000-0002-1756-2128"
        sys_meta = metadata.generate_system_metadata(pid, name, format_id, size, md5,
                                                     rights_holder)

        assert sys_meta.identifier.value() == pid
        assert sys_meta.formatId == format_id
        assert sys_meta.size == size
        assert sys_meta.submitter.value() == rights_holder
        assert sys_meta.rightsHolder.value() == rights_holder
        assert sys_meta.checksum.value() == dataoneTypes.checksum(md5).value()
        assert sys_meta.checksum.algorithm == 'MD5'
        assert sys_meta.fileName == name
