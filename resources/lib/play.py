import xbmcgui
import xbmcplugin
import utils
import config
import sys
import custom_session

_url = sys.argv[0]
_handle = int(sys.argv[1])


def play_video(params):
    """
    Determine content and pass url to Kodi for playback
    """
    try:
        with custom_session.Session() as session:
            url = config.BRIGHTCOVE_URL.format(params['id'])
            data = session.get(url).text.splitlines()
        streamurl = utils.parse_m3u8(data)
        play_item = xbmcgui.ListItem(path=streamurl)
        xbmcplugin.setResolvedUrl(_handle, True, play_item)
    except Exception as e:
        utils.handle_error('Unable to play video', exc=e)
