from typing import Annotated

import msgspec

READ_CHANNEL_CONFIGURATION = "read_channel_configuration"


class ChannelConfigurationAccess(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    other_channel: bool = False


class ChannelConfigurationQuery(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    target: Annotated[int, msgspec.Meta(ge=1)]
    layout: bool = True
