#The program finds all windows of a card game and adds a field with a random number to a corner of the window each time
# when the player moves
# Used by a player who was running 10 games simultaneously on his desktop

import random
import threading
import time
from tkinter import *

import mouse as m
import mss
import pygetwindow as gw
from mss import ScreenShotError

KEY_PIXEL_COORD = (444, 559)
KEY_PIXEL1 = (140, 18, 15)
KEY_PIXEL2 = (140, 18, 15)
DEBUG_KEY_PIXEL = False
OFFSET_X = 30
OFFSET_Y = 30


def windowTitleFilter(x: gw.Win32Window):  # function for window title filtering
    return x.title.startswith("Game")


def find_mouse():
    return m.get_position()[0], m.get_position()[1]


def generate_random_and_color():
    number = random.randint(0, 100)
    color = 'white'
    if number <= 33:
        color = 'red'
    elif 33 <= number <= 66:
        color = 'light green'
    elif 66 <= number:
        color = 'white'
    return number, color


class Reminder(object):
    def __init__(self, window: gw.Win32Window):
        self.window = window
        self._turn = False
        self.root = Tk()
        self._label = None
        self.root.overrideredirect(True)  # remove title
        self.root.geometry('+' + str(window.box.left + OFFSET_X) + '+' + str(window.box.top + OFFSET_Y))
        self.root.attributes("-topmost", True)
        self.root.withdraw()
        self.root.after_idle(self.checkWindow)  # Schedules self.show() to be called when the mainloop starts

    def sleep(self):
        self.root.after(300, self.checkWindow)  # Schedule self.show() in hide_int seconds

    def checkWindow(self):
        if self.window.isMinimized:
            self.root.after(100, self.sleep)
            self.root.withdraw()
            return

        try:
            if main_players_turn(self.window):
                if not self._turn:
                    self._turn = True
                    self._generateNumber()
                self.root.deiconify()
                self.root.geometry(
                    '+' + str(self.window.box.left + OFFSET_X) + '+' + str(self.window.box.top + OFFSET_Y))
            else:
                if self._turn:
                    self._turn = False
                    self.root.withdraw()
                    self._label.grid_forget()
            self.root.after(100, self.sleep)  # Schedule self.hide in show_int seconds
        except (ScreenShotError, gw.PyGetWindowException):
            print("Completing window tracking")
            self.root.destroy()

    def start(self):
        self.root.mainloop()

    def _generateNumber(self):
        number_and_color = generate_random_and_color()
        self._label = Label(self.root, text=str(number_and_color[0]),
                            font=("Arial Bold", 20), bg="black", fg=number_and_color[1])
        self._label.grid(column=0, row=0)

    def checkWndExist(self):
        w = self.window
        for i in gw.getAllWindows():
            if i._hWnd == w._hWnd:
                return True
        return False


def take_quick_screen_shot(w: gw.Win32Window):
    monitor = {"top": w.box.top, "left": w.box.left, "width": w.box.width, "height": w.box.height}
    return sct.grab(monitor)


def main_players_turn(w: gw.Win32Window):
    screen = take_quick_screen_shot(w)
    p = screen.pixel(KEY_PIXEL_COORD[0], KEY_PIXEL_COORD[1])
    if DEBUG_KEY_PIXEL:
        print(p)
    return p == KEY_PIXEL1 or p == KEY_PIXEL2


def createThread(w):
    r = Reminder(w)
    r.start()


def main():
    handledWindows = []
    while True:
        windows = list(filter(windowTitleFilter, gw.getAllWindows()))
        for w in windows:
            if w not in handledWindows:
                my_thread = threading.Thread(target=createThread, args=(w,))
                my_thread.start()
                handledWindows.append(w)
                print("Добавлено окно ")
        time.sleep(1)


if __name__ == '__main__':
    with mss.mss() as sct:
        print("Starting...")
        main()
