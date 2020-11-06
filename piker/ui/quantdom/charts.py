"""
Real-time quotes charting components
"""
import pyqtgraph as pg
from PyQt5 import QtCore, QtGui


class SampleLegendItem(pg.graphicsItems.LegendItem.ItemSample):

    def paint(self, p, *args):
        p.setRenderHint(p.Antialiasing)
        if isinstance(self.item, tuple):
            positive = self.item[0].opts
            negative = self.item[1].opts
            p.setPen(pg.mkPen(positive['pen']))
            p.setBrush(pg.mkBrush(positive['brush']))
            p.drawPolygon(
                QtGui.QPolygonF(
                    [
                        QtCore.QPointF(0, 0),
                        QtCore.QPointF(18, 0),
                        QtCore.QPointF(18, 18),
                    ]
                )
            )
            p.setPen(pg.mkPen(negative['pen']))
            p.setBrush(pg.mkBrush(negative['brush']))
            p.drawPolygon(
                QtGui.QPolygonF(
                    [
                        QtCore.QPointF(0, 0),
                        QtCore.QPointF(0, 18),
                        QtCore.QPointF(18, 18),
                    ]
                )
            )
        else:
            opts = self.item.opts
            p.setPen(pg.mkPen(opts['pen']))
            p.drawRect(0, 10, 18, 0.5)


class CenteredTextItem(QtGui.QGraphicsTextItem):
    def __init__(
        self,
        text='',
        parent=None,
        pos=(0, 0),
        pen=None,
        brush=None,
        valign=None,
        opacity=0.1,
    ):
        super().__init__(text, parent)

        self.pen = pen
        self.brush = brush
        self.opacity = opacity
        self.valign = valign
        self.text_flags = QtCore.Qt.AlignCenter
        self.setPos(*pos)
        self.setFlag(self.ItemIgnoresTransformations)

    def boundingRect(self):  # noqa
        r = super().boundingRect()
        if self.valign == QtCore.Qt.AlignTop:
            return QtCore.QRectF(-r.width() / 2, -37, r.width(), r.height())
        elif self.valign == QtCore.Qt.AlignBottom:
            return QtCore.QRectF(-r.width() / 2, 15, r.width(), r.height())

    def paint(self, p, option, widget):
        p.setRenderHint(p.Antialiasing, False)
        p.setRenderHint(p.TextAntialiasing, True)
        p.setPen(self.pen)
        if self.brush.style() != QtCore.Qt.NoBrush:
            p.setOpacity(self.opacity)
            p.fillRect(option.rect, self.brush)
            p.setOpacity(1)
        p.drawText(option.rect, self.text_flags, self.toPlainText())
