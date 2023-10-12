import generate from "./condition.peg";
import { ConditionParser } from "./condition-parser.service";

export class ShowCondition {
  public static readonly CONDITION_NOT_EQUALS = '!=';
  public static readonly CONDITION_EQUALS = '=';
  public static readonly CONTAINS = 'CONTAINS';

  private readonly conditions: any[];
  constructor(
    condition: string
  ) {
    this.conditions = ConditionParser.parse(condition);
  }

  public match(fields: object, path?: string): boolean {
    return ConditionParser.evaluate(fields, this.conditions, this.updatePathName(path));
  }

  private updatePathName(path?: string): string {
    if (path && path.split(/[_]+/g).length > 0) {
      /* tslint:disable-next-line */
      let [pathName, ...pathTail] = path.split(/[_]+/g);
      const pathFinalIndex = pathTail.pop();
      const pathTailString = pathTail.toString();

      pathTail = pathTail.map((value) => {
        return Number(pathFinalIndex) === Number(value) ? pathName : value;
      });

      return pathTailString !== pathTail.toString()
        ? `${pathName}_${pathTail.join('_')}_${pathFinalIndex}`
        : path;
    } else {
      return '';
    }
  }

}
