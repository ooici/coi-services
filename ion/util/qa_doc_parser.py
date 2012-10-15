
import string
import csv
from StringIO import StringIO

from interface.objects import AttachmentType
from pyon.public import IonObject, RT
from ion.util.zip import zip_of_b64
from ooi.logging import log

QA_DOCS_MANIFEST_FILE = "MANIFEST.csv"

class QADocParser(object):

    def __init__(self):
        self.csv_reader = None
        self.qa_zip_obj = None

    def prepare(self, qa_documents_zip_b64):
        self.csv_reader = None
        self.qa_zip_obj = None

        qa_zip_obj, b64err  = zip_of_b64(qa_documents_zip_b64, "qa_documents")

        if None is qa_zip_obj:
            return False, ("Base64 error: %s" % b64err)

        #parse the manifest file
        if not QA_DOCS_MANIFEST_FILE in qa_zip_obj.namelist():
            return False, ("provided qa_documents zipfile lacks manifest CSV file called %s" %
                             QA_DOCS_MANIFEST_FILE)

        log.debug("extracting manifest csv file")
        csv_contents = qa_zip_obj.read(QA_DOCS_MANIFEST_FILE)

        log.debug("parsing manifest csv file")
        try:
            dialect = csv.Sniffer().sniff(csv_contents)
        except csv.Error:
            dialect = csv.excel
        except Exception as e:
            return False, ("%s - %s", str(type(e)), str(e.args))
        csv_reader = csv.DictReader(StringIO(csv_contents), dialect=dialect)

        #validate fields in manifest file
        log.debug("validing manifest csv file")
        for f in ["filename", "name", "description", "content_type", "keywords"]:
            if not f in csv_reader.fieldnames:
                return False, ("Manifest file %s missing required field %s" %
                                 (QA_DOCS_MANIFEST_FILE, f))

        self.csv_reader = csv_reader
        self.qa_zip_obj = qa_zip_obj

        return True, ""


    def convert_to_attachments(self):

        assert(self.csv_reader is not None)
        assert(self.qa_zip_obj is not None)

        #create attachment resources for each document in the zip
        log.debug("creating attachment objects")
        attachments = []
        for row in self.csv_reader:
            att_name = row["filename"]
            att_desc = row["description"]
            att_content_type = row["content_type"]
            att_keywords = string.split(row["keywords"], ",")

            if not att_name in self.qa_zip_obj.namelist():
                return None, ("Manifest refers to a file called '%s' which is not in the zip" % att_name)

            attachments.append(IonObject(RT.Attachment,
                                         name=att_name,
                                         description=att_desc,
                                         content=self.qa_zip_obj.read(att_name),
                                         content_type=att_content_type,
                                         keywords=att_keywords,
                                         attachment_type=AttachmentType.BLOB))

        log.debug("Sanity checking manifest vs zip file")
        if len(self.qa_zip_obj.namelist()) - 1 > len(attachments):
            log.warn("There were %d files in the zip but only %d in the manifest",
                     len(self.qa_zip_obj.namelist()) - 1,
                     len(attachments))

        return attachments, ""
