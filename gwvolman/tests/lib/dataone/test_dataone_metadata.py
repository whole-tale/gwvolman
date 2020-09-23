import httmock
from gwvolman.lib.dataone.metadata import DataONEMetadata


@httmock.urlmatch(
    scheme="https",
    netloc="^cn-stage-2.test.dataone.org$",
    path="^/cn/v2/formats$",
    method="GET",
)
def mock_dataone_formats(url, request):
    response = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?><?xml-stylesheet
     type="text/xsl" href="/cn/xslt/dataone.types.v2.xsl" ?>	
<ns3:objectFormatList xmlns:ns2="http://ns.dataone.org/service/types/v1"
 xmlns:ns3="http://ns.dataone.org/service/types/v2.0" count="134" start="0" total="134">	
    <objectFormat>	
        <formatId>eml://ecoinformatics.org/eml-2.0.0</formatId>	
        <formatName>Ecological Metadata Language, version 2.0.0</formatName>	
        <formatType>METADATA</formatType>	
        <mediaType name="text/xml"/>	
        <extension>xml</extension>	
    </objectFormat>	
    <objectFormat>	
        <formatId>text/plain</formatId>	
        <formatName>Plain Text</formatName>	
        <formatType>DATA</formatType>	
        <mediaType name="text/plain"/>	
        <extension>txt</extension>	
    </objectFormat>	
    <objectFormat>	
        <formatId>image/png</formatId>	
        <formatName>Portable Network Graphics</formatName>	
        <formatType>DATA</formatType>	
        <mediaType name="image/png"/>	
        <extension>png</extension>	
    </objectFormat>	
    <objectFormat>	
        <formatId>application/octet-stream</formatId>	
        <formatName>Octet Stream</formatName>	
        <formatType>DATA</formatType>	
        <mediaType name="application/octet-stream"/>	
        <extension>data</extension>	
    </objectFormat>	
</ns3:objectFormatList>	
"""
    return httmock.response(
        status_code=200,
        content=response,
        headers={"Connection": "Close", "Content-Type": "text/xml"},
        reason=None,
        elapsed=5,
        request=request,
        stream=False,
    )


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
