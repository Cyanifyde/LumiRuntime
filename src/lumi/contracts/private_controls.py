from typing import Annotated, Literal

import msgspec

Route = Annotated[str, msgspec.Meta(pattern=r"\A[a-z][a-z0-9_]{0,23}\Z")]
Title = Annotated[str, msgspec.Meta(min_length=1, max_length=45)]


class PrivateFormSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True):


    method: Route
    title: Title
    field: Route
    label: Title
    minimum: Annotated[int, msgspec.Meta(ge=0, le=4000)]
    maximum: Annotated[int, msgspec.Meta(ge=1, le=4000)]


class PrivateTransferSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    read: Route
    close: Route


class PrivateCallSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True):
    method: Route
    scope: Literal["reservation", "upload_limit"]
    transfer: PrivateTransferSpec | None = None


class PrivateButtonSpec(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True, omit_defaults=True,
):
    route: Route
    label: Annotated[str, msgspec.Meta(min_length=1, max_length=80)]
    style: Literal["secondary", "danger"] = "secondary"
    form: PrivateFormSpec | None = None
    call: PrivateCallSpec | None = None


class PrivateMenuSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    root: Route
    title: Title
    help: Annotated[str, msgspec.Meta(min_length=1, max_length=120)]
    buttons: Annotated[tuple[PrivateButtonSpec, ...], msgspec.Meta(min_length=1, max_length=5)]


class PrivateControlCatalogue(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    schema: Literal[1]
    menus: Annotated[tuple[PrivateMenuSpec, ...], msgspec.Meta(min_length=1, max_length=16)]

    def index(self, reserved: frozenset[str]) -> dict[str, PrivateMenuSpec]:
        indexed = {menu.root: menu for menu in self.menus}
        if len(indexed) != len(self.menus) or indexed.keys() != reserved:
            raise ValueError("Invalid private command catalogue.")
        for menu in self.menus:
            if len({button.route for button in menu.buttons}) != len(menu.buttons):
                raise ValueError("Duplicate private control route.")
            for button in menu.buttons:
                if (button.form is None) == (button.call is None) or button.route == "submit":
                    raise ValueError("Invalid private control declaration.")
                if button.form and button.form.minimum > button.form.maximum:
                    raise ValueError("Invalid private form bounds.")
                if button.call and (
                    (button.call.scope == "upload_limit") != (button.call.transfer is not None)
                ):
                    raise ValueError("Invalid private transfer declaration.")
        return indexed

    def methods(self) -> frozenset[str]:
        routes = set()
        for menu in self.menus:
            for button in menu.buttons:
                if button.form:
                    routes.add(button.form.method)
                if button.call:
                    routes.add(button.call.method)
                    if button.call.transfer:
                        routes.update((button.call.transfer.read, button.call.transfer.close))
        return frozenset(routes)
