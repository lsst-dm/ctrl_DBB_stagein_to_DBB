############
ctrl_DBB_stagein_to_DBB
############

Prototype code to ingest files from the test Data Backbone (DBB) stage-in
area into the test DBB both moving file to correct location as well as
saving location, metadata, and provenance to the test DBB database.

.. warning::

   The package is a prototype and as such is subject to change and does not
   meet LSST coding standards.

****

This code is based upon the dtsfilereceiver code of the Dark Enery Survey
(DES) Data Management System (DESDM).  DESDM's mission is to process
the raw data generated by the DECAM instrument at the CTIO observatory
into science-ready data products.  The DESDM system is used for the
processing and calibration of the DES data.  In addition to managing
processing jobs, the DESDM system also includes data management items
such as metadata and provenance.

Home URL: https://des.ncsa.illinois.edu

*Created based on SVN rev: 45115 (dtsfilereceiver/trunk).*
