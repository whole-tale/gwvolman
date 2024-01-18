import hashlib
import json
import os
import uuid

import gnupg
import magic
import rfc3161ng
from pyasn1.codec.der import encoder


class TROHandler:
    def __init__(self, root, start_time, end_time):
        self.root = root
        self.start_time = start_time
        self.end_time = end_time
        try:
            self.gpg = gnupg.GPG(gnupghome=os.environ.get("GPG_HOME", "/trs"))
            self.gpg_key_id = self.gpg.list_keys().key_map[
                os.environ.get("GPG_FINGERPRINT")
            ]["keyid"]
        except KeyError:
            raise RuntimeError("Configured GPG_FINGERPRINT not found.")

    def generate_tro(self):
        tro_declaration = self.generate_tro_declaration()

        trs_signature = self.gpg.sign(
            json.dumps(tro_declaration, indent=2, sort_keys=True),
            keyid=self.gpg_key_id,
            passphrase=os.environ.get("GPG_PASSPHRASE"),
            detach=True,
        )

        with open(os.path.join(self.root, "tro.jsonld"), "w") as f:
            json.dump(tro_declaration, f, indent=2, sort_keys=True)
        with open(os.path.join(self.root, "tro.sig"), "w") as f:
            f.write(str(trs_signature))

        rt = rfc3161ng.RemoteTimestamper("https://freetsa.org/tsr", hashname="sha512")
        ts_data = {
            "tro_declaration": hashlib.sha512(
                json.dumps(tro_declaration, indent=2, sort_keys=True).encode("utf-8")
            ).hexdigest(),
            "trs_signature": hashlib.sha512(
                str(trs_signature).encode("utf-8")
            ).hexdigest(),
        }
        tsr = rt(data=json.dumps(ts_data).encode(), return_tsr=True)
        with open(os.path.join(self.root, "tro.tsr"), "wb") as f:
            f.write(encoder.encode(tsr))

    def generate_tro_declaration(self):
        tro_id = uuid.uuid5(
            uuid.NAMESPACE_URL, f"https://data.{os.environ.get('DOMAIN')}/tro.jsonld"
        )
        declaration = {
            "@context": [
                {
                    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                    "trov": "https://w3id.org/trace/2023/05/trov#",
                    "@base": f"arcp://uuid,{tro_id}/",
                }
            ],
        }

        arrangement_seq = 0
        artifacts = {}
        for path in (
            os.path.join(self.root, "version", "workspace"),
            os.path.join(self.root, "workspace"),
        ):
            # checksum all the files in the workspace
            for dirpath, dirnames, filenames in os.walk(path):
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    with open(filepath, "rb") as fp:
                        digest = hashlib.sha256(fp.read()).hexdigest()
                    if digest not in artifacts:
                        artifacts[digest] = {}
                    artifacts[digest][arrangement_seq] = filepath[len(self.root) + 1 :]
            arrangement_seq += 1

        magic_wrapper = magic.Magic(mime=True, uncompress=True)

        hasArtifacts = [
            {
                "@id": f"composition/1/artifact/{art_seq}",
                "@type": "trov:ResearchArtifact",
                "trov:mimeType": magic_wrapper.from_file(
                    f"{self.root}/{list(artifacts[digest].values())[0]}"
                )
                or "application/octet-stream",
                "trov:sha256": digest,
            }
            for art_seq, digest in enumerate(artifacts.keys())
        ]
        # sha256 of a concatenation of the sorted digests
        # of the individual digital artifacts and bitstreams
        composition_fingerprint = hashlib.sha256(
            "".join(sorted([art["trov:sha256"] for art in hasArtifacts])).encode(
                "utf-8"
            )
        ).hexdigest()

        composition = {
            "@id": "composition/1",
            "@type": "trov:ArtifactComposition",
            "trov:hasFingerprint": {
                "@id": "fingerprint",
                "@type": "trov:CompositionFingerprint",
                "trov:sha256": composition_fingerprint,
            },
            "trov:hasArtifact": hasArtifacts,
        }

        arrangements = []
        for iarr, arrangement in enumerate(
            ("Initial arrangement", "Final arrangement")
        ):
            iseq = 0
            locus = []
            for artifact in hasArtifacts:
                if iarr in artifacts[artifact["trov:sha256"]]:
                    # hasLocation needs to exclude the bag's "data/" prefix
                    locus.append(
                        {
                            "@id": f"arrangement/{iarr}/locus/{iseq}",
                            "@type": "trov:ArtifactLocus",
                            "trov:hasArtifact": {
                                "@id": artifact["@id"],
                            },
                            "trov:hasLocation": artifacts[artifact["trov:sha256"]][
                                iarr
                            ],
                        }
                    )
                    iseq += 1

            arrangements.append(
                {
                    "@id": f"arrangement/{iarr}",
                    "@type": "trov:ArtifactArrangement",
                    "rdfs:comment": arrangement,
                    "trov:hasLocus": locus,
                }
            )

        declaration["@graph"] = [
            {
                "@id": "tro",
                "@type": "trov:TransparentResearchObject",
                "trov:wasAssembledBy": {
                    "@id": "trs",
                    "@type": "trov:TrustedResearchSystem",
                    "rdfs:comment": "TRS Prototype",
                    "trov:publicKey": self.gpg.export_keys(self.gpg_key_id),
                    "trov:hasCapability": [
                        {
                            "@id": "trs/capability/1",
                            "@type": "trov:CanProvideInternetIsolation",
                        }
                    ],
                },
                "trov:wasTimestampedBy": {
                    "@id": "tsa",
                    "@type": "trov:TimeStampingAuthority",
                    "trov:sha256": (
                        "899ba3d9f777e2a74bdd34302bc06cb3f7a46ac1f565ee128f79fd5dab99d68b"
                    ),
                },
                "trov:hasAttribute": [
                    {
                        "@id": "tro/attribute/1",
                        "@type": "trov:IncludesAllInputData",
                        "trov:warrantedBy": {"@id": "trp/1/attribute/1"},
                    }
                ],
                "trov:hasComposition": composition,
                "trov:hasArrangement": arrangements,
                "trov:hasPerformance": {
                    "@id": "trp/1",
                    "@type": "trov:TrustedResearchPerformance",
                    "rdfs:comment": "Workflow execution",
                    "trov:wasConductedBy": {"@id": "trs"},
                    "trov:startedAtTime": self.start_time.isoformat(),
                    "trov:endedAtTime": self.end_time.isoformat(),
                    "trov:accessedArrangement": {"@id": "arrangement/0"},
                    "trov:modifiedArrangement": {"@id": "arrangement/1"},
                    "trov:hadPerformanceAttribute": {
                        "@id": "trp/1/attribute/1",
                        "@type": "trov:InternetIsolation",
                        "trov:warrantedBy": {"@id": "trs/capability/1"},
                    },
                },
            },
        ]

        return declaration
