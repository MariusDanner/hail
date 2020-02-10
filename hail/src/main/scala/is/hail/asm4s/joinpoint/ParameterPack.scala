package is.hail.asm4s.joinpoint

import is.hail.asm4s._
import is.hail.expr.ir
import is.hail.expr.ir.EmitTriplet
import is.hail.expr.types.physical.PType
import org.objectweb.asm.tree.InsnNode
import org.objectweb.asm.Opcodes

object ParameterPack {
  implicit val unit: ParameterPack[Unit] = new ParameterPack[Unit] {
    def push(u: Unit): Code[Unit] = Code._empty
    def newLocals(mb: MethodBuilder, name: String = null): ParameterStore[Unit] = ParameterStore.unit
    def newFields(fb: FunctionBuilder[_], name: String): ParameterStore[Unit] = ParameterStore.unit
  }

  implicit def code[T](implicit tti: TypeInfo[T]): ParameterPack[Code[T]] =
    new ParameterPack[Code[T]] {
      def push(v: Code[T]): Code[Unit] = coerce[Unit](v)
      def newLocals(mb: MethodBuilder, name: String = null): ParameterStore[Code[T]] = {
        val x = mb.newLocal(name)(tti)
        new ParameterStore[Code[T]] {
          def storeInsn: Code[Unit] = x.storeInsn
          def store(a: Code[T]): Code[Unit] = x.store(a)
          def load: Code[T] = x.load()
        }
      }
      def newFields(fb: FunctionBuilder[_], name: String): ParameterStore[Code[T]] = {
        val x = fb.newField(name)(tti)
        new ParameterStore[Code[T]] {
          def storeInsn: Code[Unit] = throw new UnsupportedOperationException("storeInsn not supported for fields")
          def store(a: Code[T]): Code[Unit] = x.store(a)
          def load: Code[T] = x.load()
        }
      }
    }

  implicit def tuple2[A, B](
    implicit ap: ParameterPack[A],
    bp: ParameterPack[B]
  ): ParameterPack[(A, B)] = new ParameterPack[(A, B)] {
    def push(v: (A, B)): Code[Unit] = Code(ap.push(v._1), bp.push(v._2))
    def newLocals(mb: MethodBuilder, name: String = null): ParameterStore[(A, B)] =
      if (name == null)
        ParameterStore.tuple(ap.newLocals(mb), bp.newLocals(mb))
      else
        ParameterStore.tuple(ap.newLocals(mb, name + "_1"),
                             bp.newLocals(mb, name + "_2"))
    def newFields(fb: FunctionBuilder[_], name: String): ParameterStore[(A, B)] =
      ParameterStore.tuple(ap.newFields(fb, name + "_1"),
                           bp.newFields(fb, name + "_2"))
  }

  implicit def tuple3[A, B, C](
    implicit ap: ParameterPack[A],
    bp: ParameterPack[B],
    cp: ParameterPack[C]
  ): ParameterPack[(A, B, C)] = new ParameterPack[(A, B, C)] {
    def push(v: (A, B, C)): Code[Unit] =
      Code(ap.push(v._1), bp.push(v._2), cp.push(v._3))
    def newLocals(mb: MethodBuilder, name: String = null): ParameterStore[(A, B, C)] =
      if (name == null)
        ParameterStore.tuple(ap.newLocals(mb), bp.newLocals(mb), cp.newLocals(mb))
      else
        ParameterStore.tuple(ap.newLocals(mb, name + "_1"),
                             bp.newLocals(mb, name + "_2"),
                             cp.newLocals(mb, name + "_3"))
    def newFields(fb: FunctionBuilder[_], name: String): ParameterStore[(A, B, C)] =
      ParameterStore.tuple(ap.newFields(fb, name + "_1"),
                           bp.newFields(fb, name + "_2"),
                           cp.newFields(fb, name + "_3"))
  }

  def array(pps: IndexedSeq[ParameterPack[_]]): ParameterPack[IndexedSeq[_]] = new ParameterPack[IndexedSeq[_]] {
    override def push(a: IndexedSeq[_]): Code[Unit] =
      pps.zip(a).foldLeft(Code._empty[Unit]) { case (acc, (pp, v)) =>
        Code(acc, pp.pushAny(v)) }

    override def newLocals(mb: MethodBuilder, name: String = null): ParameterStore[IndexedSeq[_]] =
      if (name == null)
        ParameterStore.array(pps.map(_.newLocals(mb)))
      else {
        val locals = pps.zipWithIndex.map { case (pp, i) =>
          pp.newLocals(mb, name + s"_$i")
        }
        ParameterStore.array(locals)
      }

    def newFields(fb: FunctionBuilder[_], name: String): ParameterStore[IndexedSeq[_]] = {
      val fields = pps.zipWithIndex.map { case (pp, i) =>
        pp.newFields(fb, name + s"_$i")
      }
      ParameterStore.array(fields)
    }
  }

  def let[A: ParameterPack, X](mb: MethodBuilder, a0: A)(k: A => Code[X]): Code[X] = {
    val ap = implicitly[ParameterPack[A]]
    val as = ap.newLocals(mb)
    Code(ap.push(a0), as.storeInsn, k(as.load))
  }
}

trait ParameterPack[A] {
  def push(a: A): Code[Unit]
  def pushAny(a: Any): Code[Unit] = push(a.asInstanceOf[A])
  def newLocals(mb: MethodBuilder, name: String = null): ParameterStore[A]
  def newFields(fb: FunctionBuilder[_], name: String): ParameterStore[A]
}

object ParameterStore {
  def unit: ParameterStore[Unit] = new ParameterStore[Unit] {
    def storeInsn: Code[Unit] = Code._empty
    def store(v: Unit): Code[Unit] = Code._empty
    def load: Unit = ()
  }

  def tuple[A, B](
    pa: ParameterStore[A],
    pb: ParameterStore[B]
  ): ParameterStore[(A, B)] = new ParameterStore[(A, B)] {
    def load: (A, B) = (pa.load, pb.load)
    def store(v: (A, B)): Code[Unit] = v match {
      case (a, b) => Code(pa.store(a), pb.store(b))
    }
    def storeInsn: Code[Unit] = Code(pb.storeInsn, pa.storeInsn)
  }

  def tuple[A, B, C](
    pa: ParameterStore[A],
    pb: ParameterStore[B],
    pc: ParameterStore[C]
  ): ParameterStore[(A, B, C)] = new ParameterStore[(A, B, C)] {
    def load: (A, B, C) = (pa.load, pb.load, pc.load)
    def store(v: (A, B, C)): Code[Unit] = v match {
      case (a, b, c) => Code(pa.store(a), pb.store(b), pc.store(c))
    }
    def storeInsn: Code[Unit] = Code(pc.storeInsn, pb.storeInsn, pa.storeInsn)
  }

  def array(pss: IndexedSeq[ParameterStore[_]]): ParameterStore[IndexedSeq[_]] = new ParameterStore[IndexedSeq[_]] {
    def load: IndexedSeq[_] = pss.map(_.load)
    def store(vs: IndexedSeq[_]): Code[Unit] =
      pss.zip(vs).foldLeft(Code._empty[Unit]) { case (acc, (ps, v)) => Code(acc, ps.storeAny(v)) }
    def storeInsn: Code[Unit] = pss.map(_.storeInsn).fold(Code._empty[Unit]) { case (acc, c) => Code(c, acc) } // order of c and acc is important
  }
}

abstract class ParameterStore[A] {
  private[joinpoint] def storeInsn: Code[Unit]
  def load: A
  def store(v: A): Code[Unit]

  def :=(v: A): Code[Unit] = store(v)
  def :=(cc: JoinPoint.CallCC[A]): Code[Unit] = Code(cc.code, storeInsn)
  def storeAny(v: Any): Code[Unit] = store(v.asInstanceOf[A])
}

object TypedTriplet {
  def apply(t: PType, et: EmitTriplet): TypedTriplet[t.type] =
    TypedTriplet(et.setup, et.m, et.v)

  def missing(t: PType): TypedTriplet[t.type] =
    TypedTriplet(t, EmitTriplet(Code._empty, true, ir.defaultValue(t)))

  def parameterStore[A](psm: ParameterStore[Code[Boolean]], psv: ParameterStore[Code[A]], defaultValue: Code[A]): ParameterStore[TypedTriplet[A]] = new ParameterStore[TypedTriplet[A]] {
    def load: TypedTriplet[A] = TypedTriplet(Code._empty, psm.load, psv.load)
    def store(trip: TypedTriplet[A]): Code[Unit] = Code(
      trip.setup,
      trip.m.mux(
        Code(psm.store(true), psv.store(defaultValue)),
        Code(psm.store(false), psv.storeAny(trip.v))))
    def storeInsn: Code[Unit] = ParameterStore.tuple(psv, psm).storeInsn
  }

  class Pack[P] private[joinpoint](t: PType) extends ParameterPack[TypedTriplet[P]] {
    val ppm = implicitly[ParameterPack[Code[Boolean]]]
    val ppv = ParameterPack.code(ir.typeToTypeInfo(t)).asInstanceOf[ParameterPack[Code[P]]]
    def push(trip: TypedTriplet[P]): Code[Unit] = Code(
      trip.setup,
      trip.m.mux(
        Code(coerce[Unit](ir.defaultValue(t)), coerce[Unit](const(true))),
        Code(coerce[Unit](trip.v), coerce[Unit](const(false)))))

    def newLocals(mb: MethodBuilder, name: String = null): ParameterStore[TypedTriplet[P]] = {
      val psm = ppm.newLocals(mb, if (name == null) "m" else s"${name}_missing")
      val psv = ppv.newLocals(mb, if (name == null) "v" else name)
      parameterStore[P](psm, psv, coerce[P](ir.defaultValue(t)))
    }

    def newFields(fb: FunctionBuilder[_], name: String): ParameterStore[TypedTriplet[P]] = {
      val psm = ppm.newFields(fb, if (name == null) "m" else s"${name}_missing")
      val psv = ppv.newFields(fb, if (name == null) "v" else name)
      parameterStore[P](psm, psv, coerce[P](ir.defaultValue(t)))
    }
  }

  def pack(t: PType): Pack[t.type] = new Pack(t)
}

case class TypedTriplet[P] private(setup: Code[Unit], m: Code[Boolean], v: Code[_]) {
  def untyped: EmitTriplet = EmitTriplet(setup, m, v)

  def storeTo(dm: Settable[Boolean], dv: Settable[_]): Code[Unit] =
    Code(setup, dm := m, (!dm).orEmpty(dv.storeAny(v)))
}
