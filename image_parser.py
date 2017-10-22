from PIL import ImageFile
import re

class ImageParser:
    def __init__(self):
        self.p = ImageFile.Parser()
        self.pic = b''
        self.size = None
        self.format = None

    def get_size(self):
        return self.size

    def get_format(self):
        return self.format

    def process_data(self, data):
        try:
            self.pic += data
            self.p.feed(data)
        except ValueError as err:
            raise err
        #self.format = imghdr.what(None, data)
        #if self.format == None:
        #    print(data)
        #print("self.format="+str(self.format))
        if self.p.image:
            self.size = self.p.image.size
            self.format = self.p.image.format
            return True
        return self.__check_svg(self.pic) #may be it is a svg image, that cannot be found with PIL.ImageFile
            

    def __check_svg(self, _str):
        try:
            _str = _str.decode('utf-8')
        except UnicodeDecodeError as err:
            return False
        svgAttrLinePattern = re.compile('\<svg\s+([^\>]*)\>', re.IGNORECASE)
        attrPattern = re.compile('([a-z_\-]+)\s*=\s*("[^\"]+"|\'[^\']\'|[^\s\>]+)', re.IGNORECASE)
        m = svgAttrLinePattern.search(_str)
        w = h = None
        if m is not None:
            self.format = 'SVG' 
            attributes = m.group(1)
            results = attrPattern.findall(attributes) 
            for pair in results:
                if pair[0] == 'width':
                    if re.search(r'%', pair[1]) == None:
                        res = re.findall("\d+", pair[1])
                        if len(res) != 0:
                            w = res[0]
                else:
                    if pair[0] == 'height':
                        if re.search(r'%', pair[1]) == None:
                            res = re.findall("\d+", pair[1])
                            if len(res) != 0:
                                h = res[0]
                    if h != None and w != None:
                        self.size = (w,h)
                        return True
            return True #svg with no size
        return False #not a svg
