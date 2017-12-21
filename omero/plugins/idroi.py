from omero.cli import BaseControl, CLI, ExceptionHandler

from tables import *
import omero
from omero.rtypes import rint, rstring, rlong

from parse import *

HELP = """Plugin for importing IDR ROIs"""

# Relevant columns in the HDF5 file
COLUMN_IMAGENUMBER = "ImageNumber"
COLUMN_WELLPOSITION = "Image_Metadata_CPD_WELL_POSITION"
COLUMN_PLATEID = "Image_Metadata_PlateID"
COLUMN_FIELD = "Image_Metadata_Site"

NUCLEI_LOCATION_X = "Nuclei_Location_Center_X"
NUCLEI_LOCATION_Y = "Nuclei_Location_Center_Y"
CELLS_LOCATION_X = "Cells_Location_Center_X"
CELLS_LOCATION_Y = "Cells_Location_Center_Y"
CYTOPLASM_LOCATION_X = "Cytoplasm_Location_Center_X"
CYTOPLASM_LOCATION_Y = "Cytoplasm_Location_Center_Y"

class IDROIControl(BaseControl):

    def _configure(self, parser):
        self.exc = ExceptionHandler()

        parser.add_login_arguments()

        parser.add_argument(
            "command", nargs="?",
            choices=("import", "remove"),
            help="The operation to be performed")

        parser.add_argument(
            "file",
            help="The HDF5 file")

        parser.add_argument(
            "screenId",
            help="The screen id")

        parser.add_argument(
            "--dry-run", action="store_true", help="Does not write anything to OMERO")

        parser.set_defaults(func=self.process)

    def process(self, args):
        if not args.command:
            self.ctx.die(100, "No command provided")

        if args.command == "import":
            self.importFile(args)

        if args.command == "remove":
            self.remove(args)


    def _mapImageIds(self, queryService, screenid):
        """
        Map all image names (in form 'PlateName | Well | Field')
        to their ids
        :param queryService: Reference to the query service
        :param screenid: The screen id
        :return: A dictionary mapping 'PlateName | Well | Field'
                to the image ID
        """
        params = omero.sys.Parameters()
        params.map = {"sid": rlong(screenid)}
        query = "select i.id, i.name from Screen s " \
                "right outer join s.plateLinks as pl " \
                "join pl.child as p " \
                "right outer join p.wells as w " \
                "right outer join w.wellSamples as ws " \
                "join ws.image as i " \
                "where s.id = :sid"
        imgdic = {}
        for e in queryService.projection(query, params):
            imgId = e[0].val
            imgName = e[1].val
            p = parse("{} [Well {}, Field {}]", imgName)
            imgName = "%s | %s | %s" % (p[0], p[1], p[2])
            imgdic[imgName] = imgId

        return imgdic


    def _createROIs(self, args, imgIds):
        """
        Parses the HDF5 file and creates point ROIs for nuclei, cells and cytplasm
        :param args:
        :param imgIds: Dictionary mapping image names (in form 'PlateName | Well | Field')
                       to ids
        :return: A dictionary containing a list of ROIs per image id
        """
        h5f = open_file(args.file, "r")
        try:
            imgs = h5f.get_node("/Images")
            print("Images: %d" % len(imgs))
            objs = h5f.get_node("/Objects")
            print("Objects: %d" % len(objs))

            # Map image number to image position (in form 'PlateName | Well | Field')
            imgdict = {}
            for row in imgs:
                well = row[COLUMN_WELLPOSITION]
                # Wells can have leading zero, e.g. A03, have to strip the zero
                # to match e.g. A3
                wella = well[0]
                wellb = "%d" % int(well[1:])
                well = wella + wellb
                imgpos = "%s | %s | %s" % (row[COLUMN_PLATEID], well, row[COLUMN_FIELD])
                imgdict[row[COLUMN_IMAGENUMBER]] = imgpos

            roisdict = {}
            skipped = 0
            for row in objs:
                nucleus = omero.model.RoiI()
                point = omero.model.PointI()
                point.x = row[NUCLEI_LOCATION_X]
                point.y = row[NUCLEI_LOCATION_Y]
                point.theZ = rint(0)
                point.theT = rint(0)
                point.textValue = rstring("Nucleus")
                nucleus.addShape(point)

                cell = omero.model.RoiI()
                point = omero.model.PointI()
                point.x = row[CELLS_LOCATION_X]
                point.y = row[CELLS_LOCATION_Y]
                point.theZ = rint(0)
                point.theT = rint(0)
                point.textValue = rstring("Cell")
                cell.addShape(point)

                cyto = omero.model.RoiI()
                point = omero.model.PointI()
                point.x = row[CYTOPLASM_LOCATION_X]
                point.y = row[CYTOPLASM_LOCATION_Y]
                point.theZ = rint(0)
                point.theT = rint(0)
                point.textValue = rstring("Cytoplasm")
                cyto.addShape(point)

                imgpos = imgdict[row[COLUMN_IMAGENUMBER]]
                if imgpos in imgIds:
                    if imgIds[imgpos] not in roisdict:
                        roisdict[imgIds[imgpos]] = []
                    roisdict[imgIds[imgpos]].append(nucleus)
                    roisdict[imgIds[imgpos]].append(cell)
                    roisdict[imgIds[imgpos]].append(cyto)
                else:
                    skipped += 3

            if skipped > 0:
                print("Skipped %d ROIs because they can't be associated with an image" % skipped)

        finally:
            h5f.close()

        return roisdict


    def _saveROIs(self, rois, queryService, updateService):
        """
        Save the ROIs back to OMERO
        :param rois: Dictionary of ROIs (list) per image id
        :param queryService: Reference to the query service
        :param updateService: Reference to the update service (can be None to simulate a 'dry-run')
        :return:
        """
        i = 0
        total = len(rois)
        for imageId in rois:
            i += 1
            image = queryService.get("Image", imageId)
            batch = []
            for roi in rois[imageId]:
                roi.setImage(image)
                batch.append(roi)

            if updateService:
                updateService.saveCollection(batch)
                print("Saved %d ROIs for Image %s (%d / %d images done)" % (len(batch), imageId, i, total))
            else:
                print("Dryrun - Would save %d ROIs for Image %s (%d / %d images done)" % (len(batch), imageId, i, total))


    def importFile(self, args):
        print("Import ROIs from file %s for screen %s" % (args.file, args.screenId))

        conn = self.ctx.conn(args)
        updateService = conn.sf.getUpdateService()
        queryService = conn.sf.getQueryService()

        imgdic = self._mapImageIds(queryService, args.screenId)
        print("Mapped %d image ids to plate positions" % len(imgdic))

        rois = self._createROIs(args, imgdic)
        print("Created ROIs for %d images" % len(rois))

        if args.dry_run:
            self._saveROIs(rois, queryService, None)
        else:
            self._saveROIs(rois, queryService, updateService)


    def remove(self, args):
        print("Not implemented yet")

try:
    register("idroi", IDROIControl, HELP)
except NameError:
    if __name__ == "__main__":
        cli = CLI()
        cli.register("idroi", IDROIControl, HELP)
        cli.invoke(sys.argv[1:])