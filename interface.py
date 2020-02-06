from tkinter import *
from controller import Controller
from time_thread import UpdateChecker


class Interface:
    """Esta clase es la encaragada de manejara todos los métodos realacionados con las ventanas de la aplicación.
    Esta clase debe manejar completamente el manejo de las ventanas al igual que la transferencia de información a las
    clases portafolio y controller para obtener y suministrar información"""

    def __init__(self, configuration):
        self.mainController = Controller(configuration)
        self.checker = ''
        self.root = Tk()

        # Sizing the main window
        self.root.geometry("300x300+570+150")
        self.root.resizable(0, 0)

        # Creating a menu bar inside de root(main) window
        self.menu = Menu(self.root)
        self.root.config(menu=self.menu)
        self.cascade_one = Menu(self.menu)
        self.menu.add_cascade(label='File', menu=self.cascade_one)
        self.cascade_one.add_command(label='Iniciar sesión...')
        self.cascade_one.add_separator()
        self.cascade_one.add_command(label='Exit...', command=self.root.quit)
        # Creating a new Frame
        self.frame = Frame(self.root)
        self.frame.place(relx=0.1, rely=0.1, relwidth=0.8, relheight=0.8)
        # Creating a new button
        self.button_one = Button(self.frame, text='Star file checker', command=self.start)
        self.button_one.place(relx=0.25, rely=0.3, relwidth=0.5, relheight=0.13)

        # Creating a stop button
        self.button_one = Button(self.frame, text='Stop file checker', command=self.stop)
        self.button_one.place(relx=0.25, rely=0.6, relwidth=0.5, relheight=0.13)


        # # Creating a set of labels
        # self.id_label = Label(self.root, text='Usuario:', justify=LEFT)
        # self.id_label.place(relx=0.26, rely=0.413, relwidth=0.1, relheight=0.05)
        # #
        # self.sell_stock_label = Label(self.root, text='Contraseña:')
        # self.sell_stock_label.place(relx=0.21, rely=0.493, relwidth=0.15, relheight=0.05)
        # # Creating two empty boxes for entry data
        # self.user_entry_one = Entry(self.frame)
        # self.user_entry_two = Entry(self.frame)
        # #
        # self.user_entry_one.place(relx=0.35, rely=0.4, relwidth=0.4, relheight=0.06)
        # self.user_entry_two.place(relx=0.35, rely=0.5, relwidth=0.4, relheight=0.06)

        self.root.mainloop()

    def start(self):
        self.checker = UpdateChecker(15, self.mainController.read_data)
        if not self.mainController.status and self.checker.get_status():
            self.mainController.start()
            self.checker.start()

    def stop(self):
        self.checker.stop()
        self.mainController.stop()
