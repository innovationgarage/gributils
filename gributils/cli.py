"""
Usage samples:
gributils index --database="$DATABASE" add-file --filepath="/home/saghar/IG/projects/gributils/data/smhi/arome/AM25H2_201808300600+000H00M.grib"
gributils index --database="$DATABASE" add-dir --basedir="/home/saghar/IG/projects/gributils/data/smhi/arome" 1>&2
gributils index --database="$DATABASE" lookup layers --parameter-name="Temperature" --timestamp="2018-08-30 00:04:00" 
gributils index --database="$DATABASE" lookup layers --parameter-name="P Pressure" --timestamp="2018-08-29 00:30:00" --timestamp-last-before 1 --lat 58.496206 --lon 10.2360331
gributils index --database="$DATABASE" interp-latlon --gribfile "/home/saghar/IG/projects/gributils/data/smhi/arome/AM25H2_201808300600+000H00M.grib" --layeridx 13 --lat 60. --lon 0.
gributils index --database="$DATABASE" interp-timestamp --parameter-name="Temperature" --timestamp "2018-09-12 08:00:00" --lat 60 --lon 30
"""

import click
import click_datetime
import gributils.gribindex

@click.group()
@click.pass_context
def main(ctx, **kw):
    ctx.obj = {}

@main.group()
@click.option('--database')
@click.pass_context
def index(ctx, database, **kw):
    ctx.obj = {}
    ctx.obj['index'] = gributils.gribindex.GribIndex(database)
    
@index.command()
@click.argument("output", type=click.Choice(['layers', 'names', 'units', 'level-types', 'levels']))
@click.option('--timestamp', type=click_datetime.Datetime(format='%Y-%m-%d %H:%M:%S'), default=None)
@click.option('--timestamp-last-before', type=int)
@click.option('--parameter-name')
@click.option('--parameter-unit')
@click.option('--type-of-level')
@click.option('--level', type=float)
@click.option('--level-highest-below', is_flag=True)
@click.option('--lat', type=float)
@click.option('--lon', type=float)
@click.pass_context
def lookup(ctx, **kw):
    def mangle(item):
        if hasattr(item, 'strftime'):
            return item.strftime("%Y-%m-%d %H:%M:%S")
        return str(item)
    print(ctx.obj["index"].lookup(**kw))
    # for result in ctx.obj["index"].lookup(**kw):
    #     print(result)
    
@index.command()
@click.option('--gribfile', type=str)
@click.option('--layeridx', type=str)
@click.option('--lat', type=float)
@click.option('--lon', type=float)
@click.pass_context
def interp_latlon(ctx, **kw):
    def mangle(item):
        if hasattr(item, 'strftime'):
            return item.strftime("%Y-%m-%d %H:%M:%S")
        return str(item)
    for result in ctx.obj["index"].interp_latlon(**kw):
        print(result)

@index.command()
@click.option('--timestamp', type=click_datetime.Datetime(format='%Y-%m-%d %H:%M:%S'), default=None)
@click.option('--parameter-name')
@click.option('--parameter-unit')
@click.option('--type-of-level')
@click.option('--level', type=float)
@click.option('--level-highest-below', is_flag=True)
@click.option('--lat', type=float)
@click.option('--lon', type=float)
@click.pass_context
def interp_timestamp(ctx, **kw):
    def mangle(item):
        if hasattr(item, 'strftime'):
            return item.strftime("%Y-%m-%d %H:%M:%S")
        return str(item)
    print(ctx.obj["index"].interp_timestamp(**kw))
        
@index.command()
@click.option("--filepath", type=str)
#@click.option("--add-buffer", type=float)
@click.pass_context
def add_file(ctx, **kw):
    def mangle(item):
        if hasattr(item, 'strftime'):
            return item.strftime("%Y-%m-%d %H:%M:%S")
        return str(item)
    ctx.obj["index"].add_file(**kw)

def show_error(err):
    print(err)
    
@index.command()
@click.option("--basedir", type=str)
@click.pass_context
def add_dir(ctx, **kw):
    def mangle(item):
        if hasattr(item, 'strftime'):
            return item.strftime("%Y-%m-%d %H:%M:%S")
        return str(item)
    ctx.obj["index"].add_dir(**kw, cb=show_error)
    


