using System;
using System.Collections.Generic;

namespace SMAQC
{
    abstract class Subject
    {
        private readonly List<Observer> _observers = new List<Observer>();

        public void Attach(Observer observer)
        {
            _observers.Add(observer);
        }

        public void Detach(Observer observer)
        {
            _observers.Remove(observer);
        }

        public void Notify()
        {
            foreach (var o in _observers)
            {
                o.Update();
            }
        }
    }

    abstract class Observer
    {

        public abstract void Update();

    }

    class ConcreteSubject : Subject
    {
	    // Gets or sets subject state
	    public string SubjectState { get; set; }
    }

    class ConcreteObserver : Observer
    {
        private string _observerState;
        private ConcreteSubject _subject;

        // Constructor
        public ConcreteObserver(ConcreteSubject subject)
        {
            _subject = subject;
        }

        public override void Update()
        {
            _observerState = _subject.SubjectState;
            Console.WriteLine("{0}", _observerState);
        }

        // Gets or sets subject
        public ConcreteSubject Subject
        {
            get { return _subject; }
            set { _subject = value; }
        }
    }
}
