"""
Prepare the level 2 products (surface reflectance, brightness temperature
and pixel quality) from USGS for ingestion

"""

from __future__ import absolute_import

import uuid
import logging
import yaml
import click
from osgeo import osr
import os
from os.path import join as pjoin
import shapely.affinity
import shapely.geometry
import shapely.ops
from rasterio.errors import RasterioIOError
import rasterio
import rasterio.features
from datetime import datetime
from click_datetime import Datetime
import xml.etree.cElementTree as ET
import hashlib
from pathlib import Path
import re
import tarfile
import glob

BAND_ALIASES = {'LT04': {
                       'pixel_qa': 'quality',
                       'solar_zenith_band4': 'solar_zenith_band4',
                       'solar_azimuth_band4': 'solar_azimuth_band4',
                       'sensor_zenith_band4': 'sensor_zenith_band4',
                       'sensor_azimuth_band4': 'sensor_azimuth_band4',
                       'radsat_qa': 'radsat_qa',
                       'bt_band6': 'lwir1',
                       'sr_band1': 'blue',
                       'sr_band2': 'green',
                       'sr_band3': 'red',
                       'sr_band4': 'nir',
                       'sr_band5': 'swir1',
                       'sr_band7': 'swir2',
                       'sr_atmos_opacity': 'sr_atmos_opacity',
                       'sr_cloud_qa': 'sr_cloud_qa'
                      },
                'LT05': {
                       'pixel_qa': 'quality',
                       'solar_zenith_band4': 'solar_zenith_band4',
                       'solar_azimuth_band4': 'solar_azimuth_band4',
                       'sensor_zenith_band4': 'sensor_zenith_band4',
                       'sensor_azimuth_band4': 'sensor_azimuth_band4',
                       'radsat_qa': 'radsat_qa',
                       'bt_band6': 'lwir1',
                       'sr_band1': 'blue',
                       'sr_band2': 'green',
                       'sr_band3': 'red',
                       'sr_band4': 'nir',
                       'sr_band5': 'swir1',
                       'sr_band7': 'swir2',
                       'sr_atmos_opacity': 'sr_atmos_opacity',
                       'sr_cloud_qa': 'sr_cloud_qa'
                      },
                'LE07': {
                       'pixel_qa': 'quality',
                       'solar_zenith_band4': 'solar_zenith_band4',
                       'solar_azimuth_band4': 'solar_azimuth_band4',
                       'sensor_zenith_band4': 'sensor_zenith_band4',
                       'sensor_azimuth_band4': 'sensor_azimuth_band4',
                       'radsat_qa': 'radsat_qa',
                       'bt_band6': 'lwir1',
                       'sr_band1': 'blue',
                       'sr_band2': 'green',
                       'sr_band3': 'red',
                       'sr_band4': 'nir',
                       'sr_band5': 'swir1',
                       'sr_band7': 'swir2',
                       'sr_atmos_opacity': 'sr_atmos_opacity',
                       'sr_cloud_qa': 'sr_cloud_qa'
                      },
                'LC08': {
                       'pixel_qa': 'quality',
                       'solar_zenith_band4': 'solar_zenith_band4',
                       'solar_azimuth_band4': 'solar_azimuth_band4',
                       'sensor_zenith_band4': 'sensor_zenith_band4',
                       'sensor_azimuth_band4': 'sensor_azimuth_band4',
                       'radsat_qa': 'radsat_qa',
                       'bt_band10': 'lwir1',
                       'bt_band11': 'lwir2',
                       'sr_band1': 'coastal_aerosol',
                       'sr_band2': 'blue',
                       'sr_band3': 'green',
                       'sr_band4': 'red',
                       'sr_band5': 'nir',
                       'sr_band6': 'swir1',
                       'sr_band7': 'swir2',
                       'sr_aerosol': 'sr_aerosol'
                      }
               }


def get_geo_ref(info):
    """
    Return the geographic coordinates from the metadata
    """
    
    corner_info_list = info['corner']
    for a_corner_info in corner_info_list:
        if a_corner_info['@location'] == 'UL':
            ul_x = a_corner_info['@longitude']
            ul_y = a_corner_info['@latitude']
        else:
            lr_x = a_corner_info['@longitude']
            lr_y = a_corner_info['@latitude']

    return {
        'ul': {'lat': float(ul_y), 'lon': float(ul_x)},
        'ur': {'lat': float(lr_y), 'lon': float(ul_x)},
        'll': {'lat': float(ul_y), 'lon': float(lr_x)},
        'lr': {'lat': float(lr_y), 'lon': float(lr_x)},
    }


def get_geo_ref_points(info):
    """
    Return the projected coordinates from the metadata
    """
    
    corner_point_info_list = info['corner_point']
    for a_corner_point_info in corner_point_info_list:
        if a_corner_point_info['@location'] == 'UL':
            ul_x = a_corner_point_info['@x']
            ul_y = a_corner_point_info['@y']
        else:
            lr_x = a_corner_point_info['@x']
            lr_y = a_corner_point_info['@y']

    return {
        'ul': {'x': float(ul_x), 'y': float(ul_y)},
        'ur': {'x': float(lr_x), 'y': float(ul_y)},
        'll': {'x': float(ul_x), 'y': float(lr_y)},
        'lr': {'x': float(lr_x), 'y': float(lr_y)},
    }


def valid_region(images, mask_value=None):
    """
    Return valid data region for input images based on mask value and input 
    image path
    """
    mask = None
    for fname in images:
        #logging.info("Valid regions for %s", fname)
        # ensure formats match
        with rasterio.open(str(fname), 'r') as dataset:
            transform = dataset.transform
            img = dataset.read(1)
            if mask_value is not None:
                new_mask = img & mask_value == mask_value
            else:
                new_mask = img != 0
            if mask is None:
                mask = new_mask
            else:
                mask |= new_mask
    shapes = rasterio.features.shapes(mask.astype('uint8'), mask=mask)
    shape = shapely.ops.unary_union([shapely.geometry.shape(shape) for shape, 
                                     val in shapes if val == 1])
    type(shapes)
    # convex hull
    geom = shape.convex_hull
    # buffer by 1 pixel
    geom = geom.buffer(1, join_style=3, cap_style=3)
    # simplify with 1 pixel radius
    geom = geom.simplify(1)
    # intersect with image bounding box
    geom = geom.intersection(shapely.geometry.box(0, 0, mask.shape[1], 
                                                  mask.shape[0]))
    # transform from pixel space into CRS space
    geom = shapely.affinity.affine_transform(geom, (transform.a, transform.b, 
                                                    transform.d, transform.e, 
                                                    transform.xoff, 
                                                    transform.yoff))
    return geom


def safe_valid_region(images, mask_value=None):
    """
    Safely return valid data region for input images based on mask value and 
    input image path
    """
    try:
        return valid_region(images, mask_value)
    except (OSError, RasterioIOError):
        return None


def _to_lists(x):
    """
    Returns lists of lists when given tuples of tuples
    """
    if isinstance(x, tuple):
        return [_to_lists(el) for el in x]
    return x


def strip_tag(tag):
    strip_ns_tag = tag
    split_array = tag.split('}')
    if len(split_array) > 1:
        strip_ns_tag = split_array[1]
        tag = strip_ns_tag
    return tag


def elem_to_dict(elem, strip_ns=1, strip=1):
    """
    Convert an Element into a dictionary
    
    """

    #d = OrderedDict()
    d = {}
    elem_tag = elem.tag
    if strip_ns:
        elem_tag = strip_tag(elem.tag)
    for key, value in list(elem.attrib.items()):
        d['@' + key] = value

    # loop over subelements to merge them
    for subelem in elem:
        v = elem_to_dict(subelem, strip_ns=strip_ns, strip=strip)

        tag = subelem.tag
        if strip_ns:
            tag = strip_tag(subelem.tag)

        value = v[tag]

        try:
            # add to existing list for this tag
            d[tag].append(value)
        except AttributeError:
            # turn existing entry into a list
            d[tag] = [d[tag], value]
        except KeyError:
            # add a new non-list entry
            d[tag] = value
    text = elem.text
    tail = elem.tail
    if strip:
        # ignore leading and trailing whitespace
        if text:
            text = text.strip()
        if tail:
            tail = tail.strip()

    if tail:
        d['#tail'] = tail

    if d:
        # use #text element if other attributes exist
        if text:
            d["#text"] = text
    else:
        # text is the value if no attributes
        d = text or None
    return {elem_tag: d}


def xml2dict(path):
    """
    find the xml metadata file under the product folder and convert it into
    dictionary
    
    :param path: the product folder

    :returns: the dictionary of all metadata info from original xml file
    
    """

    strip_ns = True
    strip = True

    meta = {}
    
    elem = ET.fromstring(open(path).read())
    dict_out = elem_to_dict(elem, strip_ns=strip_ns, strip=strip)
    meta = dict_out['espa_metadata']
           
    if meta == {}:
        raise RuntimeError('empty xml file')

    return meta


def get_images(bands_info, ds_path):
    """
    Extract the band info from original metadata and reconstruct them to fit
    datacube.

    :param bands_info: the bands info extracted from orginal metadata
    :param ds_path: the product folder

    :returns: the list of all sr tiff images, the dictionary of standard band 
              info and the dictionary of other band info 

    """
    
    sat = os.path.basename(ds_path)[:4]
    images = {}
    images_band = {}
    images_list = []
    bands_list = bands_info['band']
    for band in bands_list:
        image_info = {}
        image_band_info = {}
        for key, value in band.items():
            if key == '@data_type':
                value = value.lower()
            if key in ('pixel_size', 'valid_range'):
                sub_info = {}
                for sub_key, sub_value in value.items():
                    if '@' in sub_key:
                        sub_info[sub_key[1:]] = sub_value
                    else:
                        sub_info[sub_key] = sub_value
                image_info[key] = sub_info
            elif key == 'bitmap_description':
                sub_info = {}
                for sub_details in band[key]['bit']:
                    sub_info[sub_details['@num']] = sub_details['#text']
                image_info[key] = sub_info
            elif '@' in key:
                image_info[key[1:]] = value
            else:
                image_info[key] = value              
        
        image_band_info['layer'] = 1
        if Path(ds_path).suffix != '.gz':
            image_band_info['path'] = pjoin(str(Path(ds_path)), image_info['file_name'])
        else:    
            image_band_info['path'] = 'tar:{}!{}'.format(ds_path, image_info['file_name'])
        image_info.pop('file_name', None)
        
        images_band.update({BAND_ALIASES[sat][band['@name']]: image_band_info})
        images.update({BAND_ALIASES[sat][band['@name']]: image_info}) 
        # only return sr band tif files to calculate valid data bound
        if 'sr_band' in image_band_info['path']: 
            images_list.append(image_band_info['path'])            
        
    return images_list, images, images_band


def prepare_dataset(xml_path, ds_path):
    """
    Convert the product's xml metadata file into a dictionary for yaml output.
    
    :param xml_path: the xml file
    :param ds_path: the product folder

    :returns: the dictionary of metadata 

    """
    
    checksum_sha1 = hashlib.sha1(open(xml_path, 'rb').read()).hexdigest()
    info_all = xml2dict(xml_path)
    
    info_meta = info_all['global_metadata']

    sensing_time = '{}T{}'.format(info_meta['acquisition_date'], 
                                  info_meta['scene_center_time'])

    cs_code = 32600 + int(info_meta['projection_information']
                                   ['utm_proj_params']['zone_code'])
    spatial_ref = osr.SpatialReference()
    spatial_ref.ImportFromEPSG(cs_code)

    geo_ref_points = get_geo_ref_points(info_meta['projection_information'])
    geo_ref = get_geo_ref(info_meta)

    satellite = info_meta['satellite']

    images_list, images_info, images_band_info = get_images(info_all['bands'], 
                                                            ds_path)

    return {
        'id': str(uuid.uuid5(uuid.NAMESPACE_URL, xml_path)),
        'label': info_meta['product_id'],
        'checksum_sha1': checksum_sha1,
        'data_provider': info_meta['data_provider'],
        'lpgs_metadata_file': info_meta['lpgs_metadata_file'],
        'platform': {'code': satellite},
        'product_type': 'LS_USGS_L2C1',
        'instrument': {'name': info_meta['instrument']},
        'level1_production_date': info_meta['level1_production_date'],
        'solar_angles': {'unit': info_meta['solar_angles']['@units'],
                         'azimuth': info_meta['solar_angles']['@azimuth'],
                         'zenith': info_meta['solar_angles']['@zenith']},
        'earth_sun_distance': info_meta['earth_sun_distance'],
        'orientation_angle': info_meta['orientation_angle'],
        'wrs': {'row': info_meta['wrs']['@row'],
                'path': info_meta['wrs']['@path'],
                'system': info_meta['wrs']['@system']
                },
        'extent': {
            'from_dt': sensing_time,
            'to_dt': sensing_time,
            'center_dt': sensing_time,
            'coord': geo_ref,
        },
        'format': {'name': 'GeoTiff'},
        'grid_spatial': {
            'projection': {
                'geo_ref_points': geo_ref_points,
                'spatial_reference': spatial_ref.ExportToWkt(),
                'valid_data': {
                        'coordinates': _to_lists(
                            shapely.geometry.mapping(
                                shapely.ops.unary_union([
                                    safe_valid_region(images_list)
                                ])
                            )['coordinates']),
                        'type': "Polygon"}
            }
        },
        'image': {
            'bands': images_band_info,
            'bands_info': images_info
            },
        'lineage': {'source_datasets': {}}
    }


def find_xml(ds_path, output_folder):
    """
    Find the xml metadata file for the dataset (archive or not). if archive, 
    extract the xml file and store it temporally in output folder

    :param ds_path: the dataset path
    :param output_folder: the output folder

    :returns: xml with full path 

    """
 
    xml_path = ''
    if Path(ds_path).suffix != '.gz':
        for a_file in os.listdir(ds_path):
            if a_file.endswith(".xml"):
                xml_path = pjoin(ds_path, a_file)
                break
    else:
        reT = re.compile(".xml")
        tar_gz = tarfile.open(ds_path, 'r')
        members=[m for m in tar_gz.getmembers() if reT.search(m.name)]
        tar_gz.extractall(output_folder, members)
        xml_path = pjoin(output_folder, members[0].name)
        
    return xml_path
        

@click.command(help=__doc__)  
@click.option('--output', help="Write output into this directory", 
               type=click.Path(exists=False, writable=True, dir_okay=True))                       
@click.argument('input_folder',  
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
@click.option('--date', type=Datetime(format='%d/%m/%Y'), 
              default=datetime.now(), 
              help="Enter file creation start date for data preparation") 
@click.option('--checksum/--no-checksum', 
              help="Checksum the input dataset to confirm match", 
              default=False) 

def main(output, input_folder, checksum, date):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', 
                        level=logging.INFO)

    datasets = os.listdir(input_folder[0])
    for ds in datasets: 
        ds_path = pjoin(input_folder[0], ds)
        (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat(ds_path) 
        create_date = datetime.utcfromtimestamp(ctime) 
        if create_date <= date: 
            logging.info("Dataset creation time ", create_date, 
                         " is older than start date ", date, "...SKIPPING") 
        else:             
            xml_path = find_xml(ds_path, output)
            if xml_path == '':
                raise RuntimeError('no xml file under the product folder')
            logging.info("Processing %s", xml_path)            
            output_yaml = pjoin(output, '{}.yaml'.format(os.path.basename(xml_path).replace('.xml', '')))
            logging.info("Output %s", output_yaml)
            if os.path.exists(output_yaml): 
                logging.info("Output already exists %s", output_yaml) 
                with open(output_yaml) as f: 
                    if checksum: 
                        logging.info("Running checksum comparison")
                        datamap = yaml.load_all(f) 
                        for data in datamap: 
                            yaml_sha1 = data['checksum_sha1'] 
                            checksum_sha1 = hashlib.sha1(open(xml_path, 'rb').read()).hexdigest() 
                        if checksum_sha1 == yaml_sha1: 
                            logging.info("Dataset preparation already done...SKIPPING") 
                            continue 
                    else: 
                        logging.info("Dataset preparation already done...SKIPPING") 
                        continue 

            docs = prepare_dataset(xml_path, ds_path)
            with open(output_yaml, 'w') as stream:
                yaml.dump(docs, stream)

    #delete intermediate xml files for archive datasets in output folder
    xml_list = glob.glob('{}/*.xml'.format(output))
    if len(xml_list) > 0:
        for f in xml_list:
            try:
                os.remove(f)
            except OSError:
                pass
        
if __name__ == "__main__":
    main()
