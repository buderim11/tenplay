import base64
import json
import time
import urllib
import custom_session
import classes
import config


def create_authheader():
    timestr = time.strftime("%Y%m%d%H%M%S", time.gmtime())
    encoded = base64.b64encode(timestr)
    return {"X-Network-Ten-Auth": encoded}


def fetch_url(url):
    with custom_session.Session(force_tlsv1=True) as session:
        res = session.get(url, headers=create_authheader())
        return res.text


def get_shows(params):
    data = json.loads(fetch_url(config.SHOWLIST_URL))
    listing = []
    for show in data[u'Browse TV'][u'Shows']:
        x = len(show[u'Seasons'])
        multi_season = x > 1
        while x >= 1:
            s = classes.series()
            s.query = show[u'query']
            s.thumb = show[u'videoStillURL']
            s.fanart = show[u'bannerURL']
            s.season = show[u'Seasons'][x-1]
            s.genre = show[u'genre']
            if multi_season:
                s.title = '{0} Season {1}'.format(show[u'title'], s.season)
            else:
                s.title = show[u'title']
            listing.append(s)
            x -= 1
    return listing


def get_episodes(params):
    if 'page' not in params:
        page = 0
    else:
        page = params['page']
    url = config.EPISODEQUERY_URL.format(
        urllib.unquote(params['query']), page, params['season'])
    if params['category'] == 'News' or params['category'] == 'Sport':
        url = url.replace('&all=video_type_long_form:Full+Episodes', '')
    data = json.loads(fetch_url(url))
    listing = []
    for episode in data[u'items']:
        e = classes.episode()
        e.thumb = episode[u'videoStillURL']
        e.fanart = urllib.unquote(params['fanart'])
        e.title = episode[u'customFields'][u'clip_title']
        e.desc = episode[u'shortDescription']
        e.duration = episode[u'length']//1000
        e.airdate = episode[u'customFields'][u'start_date_act']
        e.page = int(page)
        e.id = episode[u'id']
        e.total_episodes = int(data['total_count'])
        if e.total_episodes > 30:
            e.query = urllib.quote(params['query'])
            e.season = params['season']
            e.category = params['category']
        listing.append(e)
    return listing


def get_featured():
    data = json.loads(fetch_url(config.FEATURED_URL))
    listing = []
    for episode in data:
        e = classes.episode()
        e.title = episode['name']
        if not e.title:
            continue
        e.thumb = episode['videoStillURL']
        e.desc = episode['short_description']
        e.id = episode['brightcoveid']
        listing.append(e)
    return listing


def get_genres():
    data = json.loads(fetch_url(config.SHOWLIST_URL))
    listing = []
    for genre in data[u'Browse TV'][u'Genres']:
        listing.append(genre)
    return listing
