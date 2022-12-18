import argparse
import pandas as pd
import re
from tqdm import tqdm
import fitz


def fix_pdf_text(old_text):
    """
    fix string from pdf reader
    """
    chr_fix_pair = {'ำ': 'า', '/า': 'ำ', '/้า': '้ำ', '/่': '่'}

    new_text = old_text.strip()
    for o, n in chr_fix_pair.items():
        new_text = new_text.replace(o, n)
    return new_text


def get_patern_of_bullet(String):
    regx = [
        ('^0[\-–\d]+$', 70),
        ('^\(\d*(\.?\d*)*\)$', 50),
        ('^[1-9]\d*(\.[1-9]\d*)*\)$', 20),
        ('^[1-9]\d*(\.[1-9]\d*)+$', 5),
        ('^[1-9]\d*\.$', 4),
        ('^โครงการ', 3),
        ('^งาน', 3),
        ('^แผนงาน', 2),
        ('^[\u0E00-\u0E7F]\.$', 1)
    ]
    
    if String in ['ด้านการจัดบริการของสำนักงานเขต',
                  'ด้านการบริหารจัดการและบริหารราชการกรุงเทพมหานคร',
                  'ด้านการศึกษา',
                  'ด้านความปลอดภัยและความเป็นระเบียบเรียบร้อย',
                  'ด้านทรัพยากรธรรมชาติและสิ่งแวดล้อม',
                  'ด้านพัฒนาสังคมและชุมชนเมือง',
                  'ด้านการสาธารณสุข',
                  'ด้านสาธารณสุข',
                  'ด้านเมืองและการพัฒนาเมือง',
                  'ด้านการระบายนำและบำบัดนำเสีย',
                  'ด้านการบริหารทั่วไป',
                  'ด้านบริหารทั่วไป',
                  'ด้านเศรษฐกิจและการพาณิชย์']: return ('ด้าน',1)

    for r, l in regx:
        if re.match(r, String):
            if l in [5, 20, 50]:
                l = String.count('.') + l
            if r == '^งาน' and String == 'งานที่จะทำ': continue
            return r, l
    return '', 0

def get_char_type(char):
    char_types = ['[\d\-]', '[\u0E00-\u0E7F]', '[a-zA-Z]']
    
    for i, ctype in enumerate(char_types):
        if re.match(ctype, char) is not None:
            return i
    return -1

def split_text(text):
    text = ['[START]'] + list(text)
    splits = list()
    token = list()
    for ci in range(len(text)-1) :
        c0, c1 = text[ci], text[ci+1]
        if get_char_type(c0) == get_char_type(c1) or c1 in [' ', '.', ','] or c0 in [',', '.'] or c0 == '[START]':
            pass
        else:
            splits.append(''.join(token))
            token = list()
        if c1 != ' ':
            token.append(c1)
    splits.append(''.join(token))
    return splits

def main(args):
    # read csv file
    bb = pd.read_csv(args.csv, index_col=0)
    bb = bb[~bb['pagenum'].isna()]

    bb['pagenum'] = bb['pagenum'].astype(int)
    bb['text'] = bb['text'].astype(str)

    if 'fisical_year' not in bb.columns:
        # add fisical_year and fix_text columns
        bb = bb.assign(
            fisical_year=bb.pdf.apply(lambda v: int(
                re.findall('/6\d/', v)[0][1:-1])),
            fix_text=bb.text.apply(fix_pdf_text)
        )

    if 'fix_text' not in bb.columns:
        # use text from ocr as fix_text
        bb.loc[~bb['ocr-text'].isna(), 'fix_text'] = bb[~bb['ocr-text'].isna()
                                                        ]['ocr-text']

    # add line label if not provided #
    if args.do_add_line_label:
      bb.loc[:, 'line_label'] = -1 
      # preprocess line number
      page_iter = tqdm(bb.groupby('image_path'), desc='parsing line')
      for g, page_df in page_iter:
          page_df = page_df.sort_values(['y0', 'x0'])
          y0_diff = page_df.y0.diff()
          page_iter.desc = f'total word: {len(page_df)}'
          if len(page_df) > 2000: continue
          bb.loc[page_df.index, 'line_label'] = ((y0_diff > 10) | y0_diff.isna()).astype(int).cumsum()

      bb.loc[:, 'line_label'] = bb.line_label.astype(int)
      bb.to_csv('with_line_label.csv', )

    bb_with_entry = bb[bb.line_label != -1]
    
    # mark entry #
    entry_label_id = 0
    is_entry = False
    prog_bar = tqdm(total=len(bb[~bb.line_label.isna()].groupby(['pdf', 'pagenum'])))

    for pdf_name, pdf_df in bb[~bb.line_label.isna()].groupby(['pdf',]): 
        doc = fitz.open(pdf_name)
    #     if pdf_name in except_pdf:
    #         prog_bar.update(len(pdf_df))
    #         continue

        for pdf_page_index, page_df in pdf_df.groupby(['pagenum']):
            prog_bar.desc = f'{pdf_name}:{pdf_page_index}:{len(page_df)}'
            prog_bar.update(1)

            page = doc.load_page(pdf_page_index)

            if [x for x in page.get_drawings() if x['rect'].height > 10]:
                prog_bar.update(len(pdf_df[pdf_df.pagenum > pdf_page_index].groupby(['pagenum'])))
                break

            bb_with_entry.loc[page_df.index, 'is_included'] = True
                
            for line_num, line_df in page_df.groupby('line_label'):
                if (line_df.fix_text == 'รายละเอียดรายจ่าย').any():
                    continue
                
                fisical_year = line_df.fisical_year.values[0]
                if (line_df.fix_text.isin(['ปี', f'25{fisical_year}', 'บาท'])).sum() == 3:
                    continue

                if line_num == 0:
                    continue

                text_line = line_df.sort_values(by='x0',ascending=True).fix_text.values
                text_line = split_text(' '.join(text_line))
                bullet_code = get_patern_of_bullet(text_line[0])[1]

                if bullet_code == 70:
                    if len(text_line) > 1:
                        second_bullet = get_patern_of_bullet(text_line[1])[1]
                        if second_bullet != 0: bullet_code = second_bullet
                    if len(text_line) > 2:
                        third_bullet = get_patern_of_bullet(text_line[2])[1]
                        if third_bullet != 0: bullet_code = third_bullet

                if bullet_code != 0:
                    entry_label_id += 1
                    bb_with_entry.loc[line_df.index, 'bullet_label'] = bullet_code
                    is_entry = True

                if is_entry:
                    bb_with_entry.loc[line_df.index, 'entry_label'] = entry_label_id

                is_last_token_baht = text_line[-1] == 'บาท'
                if is_last_token_baht:
                    is_entry = False



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--csv', help='path to CSV file that contains text in Bangkok Budget document', required=True)
    parser.add_argument(
        '--do_add_line_label',
        help='wheter not add line label to data',
        action='store_true',
        )
    args = parser.parse_args()

    main(args)
