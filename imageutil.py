#!/usr/bin/python
# -*- coding: utf-8 -*-
# Desc 图片操作
# Date: 2014-09-22


import Image
import ImageEnhance
import sys


DEBUG = True


class ImageUtil:
    LEFTTOP = 0
    RIGHTTOP = 1
    CENTER = 2
    LEFTBOTTOM = 3
    RIGHTBOTTOM = 4

    @staticmethod
    def resize_image(src, dst, rate):
        '''
            @desc 伸缩图片
            @param string src 原图片路径
            @param string dst 目标图片路径
            @param float rate 伸缩比例
            return boolean
        '''
        try:
            src_pic = Image.open(src)
            sw, sh = src_pic.size
            dw, dh = int(sw * rate), int(sh * rate)
            dst_pic = src_pic.resize((dw, dh))
            dst_pic.save(dst)
            return True
        except Exception as err:
            if DEBUG:
                print err
            return False

    @staticmethod
    def crop_image(src, dst, sx, sy, ex, ey):
        '''
            @desc 裁剪图片
            @param string src 原图片路径
            @param string dst 目标图片路径
            @param int sx 裁剪区域起始X坐标
            @param int sy 裁剪区域起始Y坐标
            @param int ex 裁剪区域结束X坐标
            @param int ey 裁剪区域结束Y坐标
            return boolean
        '''
        try:
            src_pic = Image.open(src)
            crop_box = (sx, sy, ex, ey)
            dst_pic = src_pic.crop(crop_box)
            dst_pic.save(dst)
            return True
        except Exception as err:
            if DEBUG:
                print err
            return False

    @staticmethod
    def reduce_opacity(img, opacity):
        '''
            @desc 降低图片不透明度
            @param Image img 图片
            @param float opacity 不透明度
            @return Image
        '''
        assert opacity >= 0 and opacity <= 1
        if img.mode != 'RGBA':
            img = img.convert('RGBA')
        else:
            img = img.copy()
        alpha = img.split()[3]
        alpha = ImageEnhance.Brightness(alpha).enhance(opacity)
        img.putalpha(alpha)
        return img

    @staticmethod
    def watermark_position(cls, img_size, mark_size, pos=RIGHTBOTTOM):
        '''
            @desc 获取水印区域坐标
            @param tupple img_size 图片大小
            @param tupple mark_size 水印大小
            @param int pos 水印位置
        '''
        assert pos >= 0 and pos <= 4
        padding = 10
        if cls.LEFTTOP == pos:
            mpos = (padding, padding)
        elif cls.RIGHTTOP == pos:
            mpos = (img_size[0] - mark_size[0] - padding, padding)
        elif cls.CENTER == pos:
            mpos = ((img_size[0] - mark_size[0]) / 2,
                    (img_size[1] - mark_size[1]) / 2)
        elif cls.LEFTBOTTOM == pos:
            mpos = (padding, img_size[1] - mark_size[1] - padding)
        else:
            mpos = (img_size[0] - mark_size[0] - padding,
                    img_size[1] - mark_size[1] - padding)
        return mpos

    @classmethod
    def watermark_image(cls, src, mark, dst, pos=RIGHTBOTTOM, opacity=1):
        '''
            @desc 图片添加水印
            @param string src 原图片路径
            @param string mark 水印图片路径
            @param string dst 目标图片地址
            @param int pos 水印位置
            @param float opacity 透明度
            @return boolean
        '''
        try:
            src_img = Image.open(src)
            mark_img = Image.open(mark)
            if opacity < 1:
                mark_img = cls.reduce_opacity(mark_img, opacity)
            if src_img.mode != 'RGBA':
                src_img = src_img.convert('RGBA')
            mpos = cls.watermark_position(src_img.size, mark_img.size, pos)
            layer = Image.new('RGBA', src_img.size, (0, 0, 0, 0))
            layer.paste(mark_img, mpos)
            dst_img = src_img.composite(layer, src_img, layer)
            dst_img.save(dst)
            return True
        except Exception as err:
            if DEBUG:
                print err
            return False


if '__main__' == __name__:
    print ImageUtil.LEFTTOP
    print ImageUtil.RIGHTTOP
    print ImageUtil.CENTER
    print ImageUtil.LEFTBOTTOM
    print ImageUtil.RIGHTBOTTOM
