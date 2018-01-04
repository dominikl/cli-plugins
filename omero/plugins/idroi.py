from omero.cli import BaseControl, CLI, ExceptionHandler

from tables import *
import omero
from omero.rtypes import rint, rstring, rlong

from parse import *
from time import time

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

    def _mapImagePositionToId(self, queryService, screenid):
        """
        Map all image names (in form 'PlateName | Well | Field')
        to their OMERO image ids
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

    def _mapImageNumberToPosition(self, args):
        """
        Maps the ImageNumber in the HDF5 file to plate positions
        :param args: The arguments array
        :return: A dictionary mapping the ImageNumber in the HDF5 file to
                 plate positions (in form 'PlateName | Well | Field')
        """
        imgdict = {}
        h5f = open_file(args.file, "r")
        try:
            imgs = h5f.get_node("/Images")

            # Map image number to image position (in form 'PlateName | Well | Field')
            for row in imgs:
                well = row[COLUMN_WELLPOSITION]
                # Wells can have leading zero, e.g. A03, have to strip the zero
                # to match e.g. A3
                wella = well[0]
                wellb = "%d" % int(well[1:])
                well = wella + wellb
                imgpos = "%s | %s | %s" % (row[COLUMN_PLATEID], well, row[COLUMN_FIELD])
                imgdict[row[COLUMN_IMAGENUMBER]] = imgpos

        finally:
            h5f.close()

        return imgdict

    def _saveROIs(self, rois, imgId, queryService, updateService):
        """
        Save the ROIs back to OMERO
        :param rois: A list of ROIs
        :param imgId: The image ID to attach the ROIs to
        :param queryService: Reference to the query service
        :param updateService: Reference to the update service (can be None to simulate a 'dry-run')
        :return:
        """
        try:
            image = queryService.get("Image", imgId)
            for roi in rois:
                roi.setImage(image)

            if updateService:
                updateService.saveCollection(rois)
                print("Saved %d ROIs for Image %s" % (len(rois), imgId))
            else:
                print("Dry run - Would save %d ROIs for Image %s" % (len(rois), imgId))

        except:
            print("WARNING: Could not save the ROIs for Image %s" % imgId)

    def importFile(self, args):
        print("Import ROIs from file %s for screen %s" % (args.file, args.screenId))

        conn = self.ctx.conn(args)
        updateService = conn.sf.getUpdateService()
        queryService = conn.sf.getQueryService()

        imgIds = self._mapImagePositionToId(queryService, args.screenId)
        print("Mapped %d OMERO image ids to plate positions" % len(imgIds))

        imgNumbers = self._mapImageNumberToPosition(args)
        total = len(imgNumbers)
        print("Found %d images in HDF5 file" % total)

        h5f = open_file(args.file, "a")
        try:
            objs = h5f.get_node("/Objects")
            print("Found %d objects in HDF5 file" % len(objs))
            done = 0

            if not objs.cols.ImageNumber.is_indexed:
                print("Create index for ImageNumber column...")
                objs.cols.ImageNumber.create_index()

            start = time()
            for imgNumber in imgNumbers:
                pos = imgNumbers[imgNumber]
                done += 1
                if pos in imgIds:
                    imgId = imgIds[pos]
                    rois = []
                    for row in objs.where(COLUMN_IMAGENUMBER+" == "+str(imgNumber)):
                        roi = omero.model.RoiI()
                        point = omero.model.PointI()
                        point.x = row[NUCLEI_LOCATION_X]
                        point.y = row[NUCLEI_LOCATION_Y]
                        point.theZ = rint(0)
                        point.theT = rint(0)
                        point.textValue = rstring("Nucleus")
                        roi.addShape(point)
                        rois.append(roi)

                        roi = omero.model.RoiI()
                        point = omero.model.PointI()
                        point.x = row[CELLS_LOCATION_X]
                        point.y = row[CELLS_LOCATION_Y]
                        point.theZ = rint(0)
                        point.theT = rint(0)
                        point.textValue = rstring("Cell")
                        roi.addShape(point)
                        rois.append(roi)

                        roi = omero.model.RoiI()
                        point = omero.model.PointI()
                        point.x = row[CYTOPLASM_LOCATION_X]
                        point.y = row[CYTOPLASM_LOCATION_Y]
                        point.theZ = rint(0)
                        point.theT = rint(0)
                        point.textValue = rstring("Cytoplasm")
                        roi.addShape(point)
                        rois.append(roi)

                    if args.dry_run:
                        self._saveROIs(rois, imgId, queryService, None)
                    else:
                        self._saveROIs(rois, imgId, queryService, updateService)

                else:
                    print("WARNING: Could not map image %s to an OMERO image id." % pos)

                print("%d of %d images (%d %%) processed." % (done, total, done * 100 / total))

                if done % 100 == 0:
                    duration = (time() - start) / 100
                    left = duration * (total - done)
                    m, s = divmod(left, 60)
                    h, m = divmod(m, 60)
                    start = time()
                    print("ETR: %d:%02d:%02d" % (h, m, s))

        finally:
            h5f.close()


    def remove(self, args):
        print("Not implemented yet")

try:
    register("idroi", IDROIControl, HELP)
except NameError:
    if __name__ == "__main__":
        cli = CLI()
        cli.register("idroi", IDROIControl, HELP)
        cli.invoke(sys.argv[1:])