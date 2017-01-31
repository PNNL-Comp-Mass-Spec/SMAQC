using System;
using System.Collections.Generic;

namespace SMAQC
{
    [Obsolete("Obtuse and unused")]
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

    [Obsolete("Obtuse and unused")]
    abstract class Observer
    {

        public abstract void Update();

    }

    [Obsolete("Obtuse and unused")]
    class ConcreteSubject : Subject
    {
        // Gets or sets subject state
        public string SubjectState { get; set; }
    }

    [Obsolete("Obtuse and unused")]
    class ConcreteObserver : Observer
    {
        private string _observerState;
        private ConcreteSubject _subject;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="subject"></param>
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
