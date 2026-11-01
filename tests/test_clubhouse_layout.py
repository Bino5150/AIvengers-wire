"""Mapped-window regressions for long history geometry and local scrolling."""
from __future__ import annotations

import os
import tkinter as tk
import unittest
from unittest import mock

from aivengers_wire.clubhouse import ClubhouseApp, build_identity_routes


def messages(count: int, lines: int = 3) -> list[dict]:
    return [dict(id=i, sender="demo", transport_actor="demo",
                 created_at="2026-11-01T00:00:00Z", authority="peer", owner=False,
                 provenance="direct", reply_to=i - 1 if i % 2 == 0 else None,
                 body=f"BEGIN MESSAGE {i}\n" + "Long history geometry check.\n" * lines + f"END MESSAGE {i}")
            for i in range(1, count + 1)]


@unittest.skipUnless(os.environ.get("DISPLAY"), "requires a graphical display")
class ClubhouseLayoutTests(unittest.TestCase):
    def setUp(self):
        self.root = tk.Tk()
        service = mock.Mock()
        service.routes = build_identity_routes("demo")
        service.ping.return_value = {"ok": True}
        service.channels.return_value = ["general"]
        service.read_channel.return_value = []
        self.app = ClubhouseApp(self.root, service, refresh_ms=60_000)
        # These are layout/input tests; manual refresh cases below do not need
        # an asynchronous poll or a relay connection.
        for after_id in self.root.tk.call("after", "info"):
            self.root.after_cancel(after_id)
        self.root.geometry("1100x740+40+40")
        self.root.title("Clubhouse regression check — synthetic messages")
        self.root.update()

    def tearDown(self):
        self.app.close()

    @property
    def history(self):
        return getattr(self.app, "history_text", getattr(self.app, "canvas", None))

    def load(self, rows):
        self.app.messages["general"] = rows
        self.app._render_messages()
        self.root.update()
        self.history.yview_moveto(0)
        self.root.update()

    def test_large_history_keeps_native_windows_bounded_and_all_posts_reachable(self):
        rows = messages(300, lines=36)
        self.load(rows)
        widgets = [self.root]
        for widget in widgets:
            widgets.extend(widget.winfo_children())
        self.assertLess(max(widget.winfo_height() for widget in widgets), 32768,
                        "Long history must not create an oversized native child window")
        text = self.history.get("1.0", "end-1c")
        for row in rows:
            self.assertIn(row["body"], text)
        self.assertTrue(self.history.get("@0,0", "@0,0 lineend").startswith("DEMO  #1"))
        for message_id in (150, 300):
            self.history.see(f"message_{message_id}")
            self.root.update()
            self.assertIsNotNone(self.history.dlineinfo(f"message_{message_id}"))
        self.history.yview_moveto(1)
        self.root.update()
        self.assertAlmostEqual(self.history.yview()[1], 1.0, places=4)

    def test_linux_wheel_and_small_trackpad_deltas_scroll_history(self):
        self.load(messages(40))
        before = self.history.yview()[0]
        self.history.event_generate("<Button-5>")
        self.root.update()
        self.assertGreater(self.history.yview()[0], before)
        before = self.history.yview()[0]
        self.history.event_generate("<Button-4>")
        self.root.update()
        self.assertLess(self.history.yview()[0], before)
        self.history.yview_moveto(0)
        for _ in range(12):
            self.history.event_generate("<MouseWheel>", delta=-1)
        self.root.update()
        self.assertGreater(self.history.yview()[0], 0.0, "Small deltas must accumulate, not truncate to zero")

    def test_history_wheel_does_not_hijack_composer_or_channel_list(self):
        self.load(messages(40))
        self.history.yview_moveto(0.4)
        self.root.update()
        before = self.history.yview()
        self.app.body_text.insert("1.0", "draft\n" * 30)
        for widget in (self.app.body_text, self.app.channel_list):
            widget.event_generate("<MouseWheel>", delta=-120)
            widget.event_generate("<Button-5>")
            self.root.update()
            self.assertEqual(self.history.yview(), before)
        self.assertTrue(self.app.body_text.get("1.0", "end-1c").startswith("draft"))

    def test_wheel_over_reply_button_scrolls_and_reply_keeps_its_message(self):
        self.load(messages(40))
        reply = self.history.winfo_children()[0]
        reply.event_generate("<Button-5>")
        self.root.update()
        self.assertGreater(self.history.yview()[0], 0.0)
        reply.invoke()
        self.assertEqual(self.app.reply_to, 1)
        self.assertIn("demo #1", self.app.reply_label.cget("text"))

    def test_wheel_over_history_scrollbar_moves_the_same_view(self):
        self.load(messages(40))
        scrollbar = next(widget for widget in self.history.master.winfo_children()
                         if widget.winfo_class() == "TScrollbar")
        scrollbar.event_generate("<Button-5>")
        self.root.update()
        self.assertGreater(self.history.yview()[0], 0.0)

    def test_one_tall_body_and_resize_keep_both_ends_reachable(self):
        rows = messages(1)
        rows[0]["body"] = "SINGLE BODY START\n" + "line\n" * 6000 + "SINGLE BODY END"
        self.load(rows)
        self.assertIn(rows[0]["body"], self.history.get("1.0", "end-1c"))
        for geometry in ("860x600+40+40", "1400x900+40+40"):
            self.root.geometry(geometry)
            self.root.update()
            for marker in ("SINGLE BODY START", "SINGLE BODY END"):
                index = self.history.search(marker, "1.0")
                self.history.see(index)
                self.root.update()
                self.assertIsNotNone(self.history.dlineinfo(index))
            self.assertLess(self.history.winfo_height(), 32768)

    def test_new_messages_preserve_reading_position_and_follow_only_at_bottom(self):
        self.load(messages(50))
        self.history.see("message_20")
        self.root.update()
        before = self.history.index("@0,0")
        self.app._apply_refresh("general", (["general"], messages(51)[-1:]))
        self.root.update()
        self.assertEqual(self.history.index("@0,0"), before)
        self.history.yview_moveto(1)
        self.root.update()
        self.app._apply_refresh("general", (["general"], messages(52)[-1:]))
        self.root.update()
        self.assertAlmostEqual(self.history.yview()[1], 1.0, places=4)
        self.assertEqual(self.history.cget("state"), "disabled")

    def test_reading_near_bottom_is_not_treated_as_following(self):
        self.load(messages(300, 36))
        self.history.yview_moveto(1)
        self.history.yview_scroll(-100, "pixels")
        self.root.update()
        self.assertLess(self.history.yview()[1], 1.0)
        before = self.history.index("@0,0")
        self.app._apply_refresh("general", (["general"], messages(301, 36)[-1:]))
        self.root.update()
        self.assertEqual(self.history.index("@0,0"), before)
