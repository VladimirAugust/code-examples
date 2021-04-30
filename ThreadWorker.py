# (Part of program - comporator for prices from various international telecom operators)
# Thread Worker in a desktop application that loads excel files with data
# (from telecom operators) and build a comparison table for different operators

import ntpath

from PySide6 import QtCore

from models.CLRProcessor import CLRProcessor
from models.DataRecognitionSystem import DataRecognitionSystem
from models.Sheet import Sheet


class CLRThreadWorker(QtCore.QThread):
    updateClrStatusSignal = QtCore.Signal(str)
    updateCLRTableWidgetSignal = QtCore.Signal(int)
    addUndefinedSheetListSignal = QtCore.Signal(Sheet)
    clearUndefinedSheetListSignal = QtCore.Signal()
    updateSheetErrorsSignal = QtCore.Signal(list)

    def __init__(self, dataRecognitionSystem: DataRecognitionSystem):
        QtCore.QThread.__init__(self)
        self._clr = CLRProcessor()
        self._dataRecognitionSystem = dataRecognitionSystem
        self._selectedFiles = None
        self._undefinedSheets = None
        self._definedSheets = None
        self._workMode = 1

    def setWorkMode1(self):
        self._workMode = 1
        self._clr.clearCodeFilter()
        self._clr.clearRateFilter()

    def setWorkMode2(self, destination, code, rate):
        self._workMode = 2
        self._clr.clearCodeFilter()
        self._clr.clearRateFilter()
        if (destination != '' and not destination.isspace()) or (code != '' and not code.isspace()):
            self._clr.setCodeFilter(destination, code)
        if rate != '' and not rate.isspace():
            self._clr.setRateFilter(float(rate))

    def setFiles(self, files):
        self._selectedFiles = files

    def updateDataForUndefinedSheets(self, data):
        self._dataRecognitionSystem.setUserAliases(data)
        changes = False
        for sheet in self._undefinedSheets:
            self._dataRecognitionSystem.determineSheetData(sheet)
            if sheet.isDataFormatDefined():
                changes = True
                self._clr.addSheet(sheet)
                self.updateClrStatusSignal.emit(
                    f"Column definition succeeded for the sheet {sheet.filename}")
                self._definedSheets.append(sheet)
                self._undefinedSheets.remove(sheet)
            else:
                self.updateClrStatusSignal.emit(
                    f"Coudn't define data in the sheet {sheet.filename}")
        if changes:
            self._clr.buildCLR()
            self.updateCLRTableWidgetSignal.emit(1)
            self._updateUndefinedSheetListUI()

    def run(self):
        self._definedSheets = []
        self._undefinedSheets = []
        self.updateCLRTableWidgetSignal.emit(0)
        self._clr.removeAllSheets()
        for file in self._selectedFiles:
            self.updateClrStatusSignal.emit(f"Loading the file {ntpath.basename(file)}...")
            sheet = Sheet(file)
            if not sheet.load():
                self.updateClrStatusSignal.emit(f"An error occurred while opening the file {ntpath.basename(file)}. This file may be corrupted or is not an excel file");
                continue
            self._dataRecognitionSystem.determineSheetData(sheet)
            sheet.printInfo()
            if not sheet.isDataFormatDefined():
                print("Coudn't define data in the sheet ", sheet)
                self._undefinedSheets.append(sheet)
            else:
                self._definedSheets.append(sheet)
                self._clr.addSheet(sheet)

        self._updateUndefinedSheetListUI()

        if not self._definedSheets and not self._undefinedSheets:
            return
        if len(self._definedSheets) == 0:
            self.updateClrStatusSignal.emit("Please set manually aliases for unparsed sheets")
            return

        self.updateClrStatusSignal.emit("Parsing data...")
        self._clr.buildCLR()
        self.updateClrStatusSignal.emit("Populating result table...")
        self.updateCLRTableWidgetSignal.emit(1)
        self.updateSheetErrorsSignal.emit(self._clr.getErrors())
        if len(self._undefinedSheets) == 0:
            self.updateClrStatusSignal.emit("Done. All sheets were parsed successfully")
        else:
            self.updateClrStatusSignal.emit("Done. Please set manually aliases for unparsed sheets")

    def _updateUndefinedSheetListUI(self):
        self.clearUndefinedSheetListSignal.emit()
        for sheet in self._undefinedSheets:
            self.addUndefinedSheetListSignal.emit(sheet)

    def getCLR(self):
        return self._clr

    def getUndefinedSheetSheetnames(self, num):
        return self._undefinedSheets[num].sheetnames
