import logging
import sentry_sdk
from sentry_sdk.integrations.pyramid import PyramidIntegration

from slovar import slovar

log = logging.getLogger(__name__)

def includeme(config):
    Settings = slovar(config.registry.settings)

    if Settings.asbool('sentry.enable', default=False):
        log.info('Sentry Enabled')
        sentry_sdk.init(
             dsn = Settings['sentry.dsn'],
             send_default_pii = True,
             integrations = [PyramidIntegration()]
        )
    else:
        log.info('Sentry Disabled')

